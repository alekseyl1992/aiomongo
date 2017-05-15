import pytest
from bson.objectid import ObjectId
from bson.son import SON
from pymongo.bulk import BulkWriteError
from pymongo.errors import InvalidDocument, InvalidOperation
from pymongo.operations import *

from tests.utils import oid_generated_on_client


def assert_equal_response(expected, actual):
    """Compare response from bulk.execute() to expected response."""
    for key, value in expected.items():
        if key == 'nModified':
            assert value == actual['nModified']
        elif key == 'upserted':
            expected_upserts = value
            actual_upserts = actual['upserted']
            assert len(expected_upserts) == len(actual_upserts)

            for e, a in zip(expected_upserts, actual_upserts):
                assert_equal_upsert(e, a)

        elif key == 'writeErrors':
            expected_errors = value
            actual_errors = actual['writeErrors']
            assert len(expected_errors) == len(actual_errors)

            for e, a in zip(expected_errors, actual_errors):
                assert_equal_write_error(e, a)

        else:
            assert actual.get(key) == value


def assert_equal_upsert(expected, actual):
    """Compare bulk.execute()['upserts'] to expected value.

    Like: {'index': 0, '_id': ObjectId()}
    """
    assert expected['index'] == actual['index']
    if expected['_id'] == '...':
        # Unspecified value.
        assert '_id' in actual
    else:
        assert expected['_id'] == actual['_id']


def assert_equal_write_error(expected, actual):
    """Compare bulk.execute()['writeErrors'] to expected value.

    Like: {'index': 0, 'code': 123, 'errmsg': '...', 'op': { ... }}
    """
    assert expected['index'] == actual['index']
    assert expected['code'] == actual['code']
    if expected['errmsg'] == '...':
        # Unspecified value.
        assert 'errmsg' in actual
    else:
        assert expected['errmsg'] == actual['errmsg']

    expected_op = expected['op'].copy()
    actual_op = actual['op'].copy()
    if expected_op.get('_id') == '...':
        # Unspecified _id.
        assert '_id' in actual_op
        actual_op.pop('_id')
        expected_op.pop('_id')

    assert expected_op == actual_op


class TestBulk:

    @pytest.mark.asyncio
    async def test_empty(self, test_coll):
        bulk = test_coll.initialize_ordered_bulk_op()
        with pytest.raises(InvalidOperation):
            await bulk.execute()

    @pytest.mark.asyncio
    async def test_find(self, test_coll):
        # find() requires a selector.
        bulk = test_coll.initialize_ordered_bulk_op()
        with pytest.raises(TypeError):
            bulk.find()
        with pytest.raises(TypeError):
            bulk.find('foo')
        # No error.
        bulk.find({})

    @pytest.mark.asyncio
    async def test_bypass_document_validation_bulk_op(self, test_db, test_coll, mongo_version):

        if not mongo_version.at_least(3, 1, 9, -1):
            return pytest.skip('Not supported on this mongo version')

        # Test insert
        await test_coll.insert_one({'z': 0})
        await test_db.command(SON([('collMod', 'test'),
                                   ('validator', {'z': {'$gte': 0}})]))
        bulk = test_coll.initialize_ordered_bulk_op(
            bypass_document_validation=False)
        bulk.insert({'z': -1}) # error
        with pytest.raises(BulkWriteError):
            await bulk.execute()
        assert 0 == await test_coll.count({'z': -1})

        bulk = test_coll.initialize_ordered_bulk_op(
            bypass_document_validation=True)
        bulk.insert({'z': -1})
        await bulk.execute()
        assert 1 == await test_coll.count({'z': -1})

        await test_coll.insert_one({'z': 0})
        await test_db.command(SON([('collMod', 'test'),
                                   ('validator', {'z': {'$gte': 0}})]))
        bulk = test_coll.initialize_unordered_bulk_op(
            bypass_document_validation=False)
        bulk.insert({'z': -1}) # error
        with pytest.raises(BulkWriteError):
            await bulk.execute()
        assert 1 == await test_coll.count({'z': -1})

        bulk = test_coll.initialize_unordered_bulk_op(
            bypass_document_validation=True)
        bulk.insert({'z': -1})
        await bulk.execute()
        assert 2 == await test_coll.count({'z': -1})
        await test_coll.drop()

    @pytest.mark.asyncio
    async def test_insert(self, test_coll):
        expected = {
            'nMatched': 0,
            'nModified': 0,
            'nUpserted': 0,
            'nInserted': 1,
            'nRemoved': 0,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }

        bulk = test_coll.initialize_ordered_bulk_op()
        with pytest.raises(TypeError):
            bulk.insert(1)

        # find() before insert() is prohibited.
        with pytest.raises(AttributeError):
            bulk.find({}).insert({})

        # We don't allow multiple documents per call.
        with pytest.raises(TypeError):
            bulk.insert([{}, {}])
        with pytest.raises(TypeError):
            bulk.insert(({} for _ in range(2)))

        bulk.insert({})
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert 1 == await test_coll.count()
        doc = await test_coll.find_one()
        assert oid_generated_on_client(doc['_id'])

        bulk = test_coll.initialize_unordered_bulk_op()
        bulk.insert({})
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert 2 == await test_coll.count()

        result = await test_coll.bulk_write([InsertOne({})])
        assert_equal_response(expected, result.bulk_api_result)
        assert 1 == result.inserted_count
        assert 3 == await test_coll.count()

    @pytest.mark.asyncio
    async def test_insert_check_keys(self, test_coll):
        bulk = test_coll.initialize_ordered_bulk_op()
        bulk.insert({'$dollar': 1})
        with pytest.raises(InvalidDocument):
            await bulk.execute()

        bulk = test_coll.initialize_ordered_bulk_op()
        bulk.insert({'a.b': 1})
        with pytest.raises(InvalidDocument):
            await bulk.execute()

    @pytest.mark.asyncio
    async def test_update(self, test_coll):

        expected = {
            'nMatched': 2,
            'nModified': 2,
            'nUpserted': 0,
            'nInserted': 0,
            'nRemoved': 0,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }
        await test_coll.insert_many([{}, {}])

        bulk = test_coll.initialize_ordered_bulk_op()

        # update() requires find() first.
        with pytest.raises(AttributeError):
            bulk.update({'$set': {'x': 1}})

        with pytest.raises(TypeError):
            bulk.find({}).update(1)
        with pytest.raises(ValueError):
            bulk.find({}).update({})

        # All fields must be $-operators.
        with pytest.raises(ValueError):
            bulk.find({}).update({'foo': 'bar'})
        bulk.find({}).update({'$set': {'foo': 'bar'}})
        result = await bulk.execute()
        assert_equal_response(expected, result)
        assert await test_coll.find({'foo': 'bar'}).count() == 2

        await test_coll.delete_many({})
        await test_coll.insert_many([{}, {}])
        result = await test_coll.bulk_write([UpdateMany({},
                                                  {'$set': {'foo': 'bar'}})])
        assert_equal_response(expected, result.bulk_api_result)
        assert 2 == result.matched_count
        assert result.modified_count in (2, None)

        # All fields must be $-operators -- validated server-side.
        bulk = test_coll.initialize_ordered_bulk_op()
        updates = SON([('$set', {'x': 1}), ('y', 1)])
        bulk.find({}).update(updates)
        with pytest.raises(BulkWriteError):
            await bulk.execute()

        await test_coll.delete_many({})
        await test_coll.insert_many([{}, {}])

        bulk = test_coll.initialize_unordered_bulk_op()
        bulk.find({}).update({'$set': {'bim': 'baz'}})
        result = await bulk.execute()
        assert_equal_response(
            {'nMatched': 2,
             'nModified': 2,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        assert await test_coll.find({'bim': 'baz'}).count() == 2

        await test_coll.insert_one({'x': 1})
        bulk = test_coll.initialize_unordered_bulk_op()
        bulk.find({'x': 1}).update({'$set': {'x': 42}})
        result = await bulk.execute()
        assert_equal_response(
            {'nMatched': 1,
             'nModified': 1,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        assert 1 == await test_coll.find({'x': 42}).count()

        # Second time, x is already 42 so nModified is 0.
        bulk = test_coll.initialize_unordered_bulk_op()
        bulk.find({'x': 42}).update({'$set': {'x': 42}})
        result = await bulk.execute()
        assert_equal_response(
            {'nMatched': 1,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

    @pytest.mark.asyncio
    async def test_update_one(self, test_coll):

        expected = {
            'nMatched': 1,
            'nModified': 1,
            'nUpserted': 0,
            'nInserted': 0,
            'nRemoved': 0,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }

        await test_coll.insert_many([{}, {}])

        bulk = test_coll.initialize_ordered_bulk_op()

        # update_one() requires find() first.
        with pytest.raises(AttributeError):
            bulk.update_one({'$set': {'x': 1}})

        with pytest.raises(TypeError):
            bulk.find({}).update_one(1)
        with pytest.raises(ValueError):
            bulk.find({}).update_one({})
        with pytest.raises(ValueError):
            bulk.find({}).update_one({'foo': 'bar'})
        bulk.find({}).update_one({'$set': {'foo': 'bar'}})
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert await test_coll.find({'foo': 'bar'}).count() == 1

        await test_coll.delete_many({})
        await test_coll.insert_many([{}, {}])
        result = await test_coll.bulk_write([UpdateOne({},
                                                 {'$set': {'foo': 'bar'}})])
        assert_equal_response(expected, result.bulk_api_result)
        assert 1 == result.matched_count
        assert result.modified_count in (1, None)

        await test_coll.delete_many({})
        await test_coll.insert_many([{}, {}])

        bulk = test_coll.initialize_unordered_bulk_op()
        bulk.find({}).update_one({'$set': {'bim': 'baz'}})
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert await test_coll.find({'bim': 'baz'}).count() == 1

        # All fields must be $-operators -- validated server-side.
        bulk = test_coll.initialize_ordered_bulk_op()
        updates = SON([('$set', {'x': 1}), ('y', 1)])
        bulk.find({}).update_one(updates)
        with pytest.raises(BulkWriteError):
            await bulk.execute()

    @pytest.mark.asyncio
    async def test_replace_one(self, test_coll):

        expected = {
            'nMatched': 1,
            'nModified': 1,
            'nUpserted': 0,
            'nInserted': 0,
            'nRemoved': 0,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }

        await test_coll.insert_many([{}, {}])

        bulk = test_coll.initialize_ordered_bulk_op()
        with pytest.raises(TypeError):
            bulk.find({}).replace_one(1)
        with pytest.raises(ValueError):
            bulk.find({}).replace_one({'$set': {'foo': 'bar'}})
        bulk.find({}).replace_one({'foo': 'bar'})
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert await test_coll.find({'foo': 'bar'}).count() == 1

        await test_coll.delete_many({})
        await test_coll.insert_many([{}, {}])
        result = await test_coll.bulk_write([ReplaceOne({}, {'foo': 'bar'})])
        assert_equal_response(expected, result.bulk_api_result)
        assert 1 == result.matched_count
        assert result.modified_count in (1, None)

        await test_coll.delete_many({})
        await test_coll.insert_many([{}, {}])

        bulk = test_coll.initialize_unordered_bulk_op()
        bulk.find({}).replace_one({'bim': 'baz'})
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert await test_coll.find({'bim': 'baz'}).count() == 1

    @pytest.mark.asyncio
    async def test_remove(self, test_coll):
        # Test removing all documents, ordered.
        expected = {
            'nMatched': 0,
            'nModified': 0,
            'nUpserted': 0,
            'nInserted': 0,
            'nRemoved': 2,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }
        await test_coll.insert_many([{}, {}])

        bulk = test_coll.initialize_ordered_bulk_op()

        # remove() must be preceded by find().
        with pytest.raises(AttributeError):
            bulk.remove()
        bulk.find({}).remove()
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert await test_coll.count() == 0

        await test_coll.insert_many([{}, {}])
        result = await test_coll.bulk_write([DeleteMany({})])
        assert_equal_response(expected, result.bulk_api_result)
        assert 2 == result.deleted_count

        # Test removing some documents, ordered.
        await test_coll.insert_many([{}, {'x': 1}, {}, {'x': 1}])

        bulk = test_coll.initialize_ordered_bulk_op()

        bulk.find({'x': 1}).remove()
        result = await bulk.execute()
        assert_equal_response(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 2,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        assert await test_coll.count() == 2
        await test_coll.delete_many({})

        # Test removing all documents, unordered.
        await test_coll.insert_many([{}, {}])

        bulk = test_coll.initialize_unordered_bulk_op()
        bulk.find({}).remove()
        result = await bulk.execute()
        assert_equal_response(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 2,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        # Test removing some documents, unordered.
        assert await test_coll.count() == 0

        await test_coll.insert_many([{}, {'x': 1}, {}, {'x': 1}])

        bulk = test_coll.initialize_unordered_bulk_op()
        bulk.find({'x': 1}).remove()
        result = await bulk.execute()
        assert_equal_response(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 2,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        assert await test_coll.count() == 2
        await test_coll.delete_many({})

    @pytest.mark.asyncio
    async def test_remove_one(self, test_coll):

        bulk = test_coll.initialize_ordered_bulk_op()

        # remove_one() must be preceded by find().
        with pytest.raises(AttributeError):
            bulk.remove_one()

        # Test removing one document, empty selector.
        # First ordered, then unordered.
        await test_coll.insert_many([{}, {}])
        expected = {
            'nMatched': 0,
            'nModified': 0,
            'nUpserted': 0,
            'nInserted': 0,
            'nRemoved': 1,
            'upserted': [],
            'writeErrors': [],
            'writeConcernErrors': []
        }

        bulk.find({}).remove_one()
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert await test_coll.count() == 1

        await test_coll.insert_one({})
        result = await test_coll.bulk_write([DeleteOne({})])
        assert_equal_response(expected, result.bulk_api_result)
        assert 1 == result.deleted_count
        assert await test_coll.count() == 1

        await test_coll.insert_one({})

        bulk = test_coll.initialize_unordered_bulk_op()
        bulk.find({}).remove_one()
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert await test_coll.count() == 1

        # Test removing one document, with a selector.
        # First ordered, then unordered.
        await test_coll.insert_one({'x': 1})

        bulk = test_coll.initialize_ordered_bulk_op()
        bulk.find({'x': 1}).remove_one()
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert [{}] == await test_coll.find({}, {'_id': False}).to_list()
        await test_coll.insert_one({'x': 1})

        bulk = test_coll.initialize_unordered_bulk_op()
        bulk.find({'x': 1}).remove_one()
        result = await bulk.execute()
        assert_equal_response(expected, result)

        assert [{}] == await test_coll.find({}, {'_id': False}).to_list()

    @pytest.mark.asyncio
    async def test_upsert(self, test_coll):
        bulk = test_coll.initialize_ordered_bulk_op()

        # upsert() requires find() first.
        with pytest.raises(AttributeError):
            bulk.upsert()

        expected = {
            'nMatched': 0,
            'nModified': 0,
            'nUpserted': 1,
            'nInserted': 0,
            'nRemoved': 0,
            'upserted': [{'index': 0, '_id': '...'}]
        }

        # Note, in MongoDB 2.4 the server won't return the
        # "upserted" field unless _id is an ObjectId
        bulk.find({}).upsert().replace_one({'foo': 'bar'})
        result = await bulk.execute()
        assert_equal_response(expected, result)

        await test_coll.delete_many({})
        result = await test_coll.bulk_write([ReplaceOne({},
                                                  {'foo': 'bar'},
                                                  upsert=True)])
        assert_equal_response(expected, result.bulk_api_result)
        assert 1 == result.upserted_count
        assert 1 == len(result.upserted_ids)
        assert isinstance(result.upserted_ids.get(0), ObjectId)

        assert await test_coll.find({'foo': 'bar'}).count() == 1

        bulk = test_coll.initialize_ordered_bulk_op()
        bulk.find({}).upsert().update_one({'$set': {'bim': 'baz'}})
        result = await bulk.execute()
        assert_equal_response(
            {'nMatched': 1,
             'nModified': 1,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        assert await test_coll.find({'bim': 'baz'}).count() == 1

        bulk = test_coll.initialize_ordered_bulk_op()
        bulk.find({}).upsert().update({'$set': {'bim': 'bop'}})
        # Non-upsert, no matches.
        bulk.find({'x': 1}).update({'$set': {'x': 2}})
        result = await bulk.execute()
        assert_equal_response(
            {'nMatched': 1,
             'nModified': 1,
             'nUpserted': 0,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

        assert await test_coll.find({'bim': 'bop'}).count() == 1
        assert await test_coll.find({'x': 2}).count() == 0

    @pytest.mark.asyncio
    async def test_upsert_large(self, mongo, test_coll):
        connection = await mongo.get_connection()
        big = 'a' * (connection.max_bson_size - 37)
        bulk = test_coll.initialize_ordered_bulk_op()
        bulk.find({'x': 1}).upsert().update({'$set': {'s': big}})
        result = await bulk.execute()
        assert_equal_response(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 1,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [{'index': 0, '_id': '...'}]},
            result)

        assert 1 == await test_coll.find({'x': 1}).count()

    @pytest.mark.asyncio
    async def test_client_generated_upsert_id(self, test_coll):
        batch = test_coll.initialize_ordered_bulk_op()
        batch.find({'_id': 0}).upsert().update_one({'$set': {'a': 0}})
        batch.find({'a': 1}).upsert().replace_one({'_id': 1})
        batch.find({'_id': 2}).upsert().replace_one({'_id': 2})
        result = await batch.execute()
        assert_equal_response(
            {'nMatched': 0,
             'nModified': 0,
             'nUpserted': 3,
             'nInserted': 0,
             'nRemoved': 0,
             'upserted': [{'index': 0, '_id': 0},
                          {'index': 1, '_id': 1},
                          {'index': 2, '_id': 2}]},
            result)

    @pytest.mark.asyncio
    async def test_single_ordered_batch(self, test_coll):
        batch = test_coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1})
        batch.find({'a': 1}).update_one({'$set': {'b': 1}})
        batch.find({'a': 2}).upsert().update_one({'$set': {'b': 2}})
        batch.insert({'a': 3})
        batch.find({'a': 3}).remove()
        result = await batch.execute()
        assert_equal_response(
            {'nMatched': 1,
             'nModified': 1,
             'nUpserted': 1,
             'nInserted': 2,
             'nRemoved': 1,
             'upserted': [{'index': 2, '_id': '...'}]},
            result)

    @pytest.mark.asyncio
    async def test_single_error_ordered_batch(self, test_coll):
        await test_coll.create_index('a', unique=True)
        try:
            batch = test_coll.initialize_ordered_bulk_op()
            batch.insert({'b': 1, 'a': 1})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
            batch.insert({'b': 3, 'a': 2})

            try:
                await batch.execute()
            except BulkWriteError as exc:
                result = exc.details
                assert exc.code == 65
            else:
                pytest.fail('Error not raised')

            assert_equal_response(
                {'nMatched': 0,
                 'nModified': 0,
                 'nUpserted': 0,
                 'nInserted': 1,
                 'nRemoved': 0,
                 'upserted': [],
                 'writeConcernErrors': [],
                 'writeErrors': [
                     {'index': 1,
                      'code': 11000,
                      'errmsg': '...',
                      'op': {'q': {'b': 2},
                             'u': {'$set': {'a': 1}},
                             'multi': False,
                             'upsert': True}}]},
                result)
        finally:
            await test_coll.drop_index([('a', 1)])

    @pytest.mark.asyncio
    async def test_multiple_error_ordered_batch(self, test_coll):
        await test_coll.create_index('a', unique=True)
        try:
            batch = test_coll.initialize_ordered_bulk_op()
            batch.insert({'b': 1, 'a': 1})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
            batch.find({'b': 3}).upsert().update_one({'$set': {'a': 2}})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
            batch.insert({'b': 4, 'a': 3})
            batch.insert({'b': 5, 'a': 1})

            try:
                await batch.execute()
            except BulkWriteError as exc:
                result = exc.details
                assert exc.code == 65
            else:
                pytest.fail('Error not raised')

            assert_equal_response(
                {'nMatched': 0,
                 'nModified': 0,
                 'nUpserted': 0,
                 'nInserted': 1,
                 'nRemoved': 0,
                 'upserted': [],
                 'writeConcernErrors': [],
                 'writeErrors': [
                     {'index': 1,
                      'code': 11000,
                      'errmsg': '...',
                      'op': {'q': {'b': 2},
                             'u': {'$set': {'a': 1}},
                             'multi': False,
                             'upsert': True}}]},
                result)
        finally:
            await test_coll.drop_index([('a', 1)])

    @pytest.mark.asyncio
    async def test_single_unordered_batch(self, test_coll):
        batch = test_coll.initialize_unordered_bulk_op()
        batch.insert({'a': 1})
        batch.find({'a': 1}).update_one({'$set': {'b': 1}})
        batch.find({'a': 2}).upsert().update_one({'$set': {'b': 2}})
        batch.insert({'a': 3})
        batch.find({'a': 3}).remove()
        result = await batch.execute()
        assert_equal_response(
            {'nMatched': 1,
             'nModified': 1,
             'nUpserted': 1,
             'nInserted': 2,
             'nRemoved': 1,
             'upserted': [{'index': 2, '_id': '...'}],
             'writeErrors': [],
             'writeConcernErrors': []},
            result)

    @pytest.mark.asyncio
    async def test_single_error_unordered_batch(self, test_coll):
        await test_coll.create_index('a', unique=True)
        try:
            batch = test_coll.initialize_unordered_bulk_op()
            batch.insert({'b': 1, 'a': 1})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 1}})
            batch.insert({'b': 3, 'a': 2})

            try:
                await batch.execute()
            except BulkWriteError as exc:
                result = exc.details
                assert exc.code == 65
            else:
                pytest.fail('Error not raised')

            assert_equal_response(
                {'nMatched': 0,
                 'nModified': 0,
                 'nUpserted': 0,
                 'nInserted': 2,
                 'nRemoved': 0,
                 'upserted': [],
                 'writeConcernErrors': [],
                 'writeErrors': [
                     {'index': 1,
                      'code': 11000,
                      'errmsg': '...',
                      'op': {'q': {'b': 2},
                             'u': {'$set': {'a': 1}},
                             'multi': False,
                             'upsert': True}}]},
                result)
        finally:
            await test_coll.drop_index([('a', 1)])

    @pytest.mark.asyncio
    async def test_multiple_error_unordered_batch(self, test_coll):
        await test_coll.create_index('a', unique=True)
        try:
            batch = test_coll.initialize_unordered_bulk_op()
            batch.insert({'b': 1, 'a': 1})
            batch.find({'b': 2}).upsert().update_one({'$set': {'a': 3}})
            batch.find({'b': 3}).upsert().update_one({'$set': {'a': 4}})
            batch.find({'b': 4}).upsert().update_one({'$set': {'a': 3}})
            batch.insert({'b': 5, 'a': 2})
            batch.insert({'b': 6, 'a': 1})

            try:
                await batch.execute()
            except BulkWriteError as exc:
                result = exc.details
                assert exc.code == 65
            else:
                pytest.fail('Error not raised')
            # Assume the update at index 1 runs before the update at index 3,
            # although the spec does not require it. Same for inserts.
            assert_equal_response(
                {'nMatched': 0,
                 'nModified': 0,
                 'nUpserted': 2,
                 'nInserted': 2,
                 'nRemoved': 0,
                 'upserted': [
                     {'index': 1, '_id': '...'},
                     {'index': 2, '_id': '...'}],
                 'writeConcernErrors': [],
                 'writeErrors': [
                     {'index': 3,
                      'code': 11000,
                      'errmsg': '...',
                      'op': {'q': {'b': 4},
                             'u': {'$set': {'a': 3}},
                             'multi': False,
                             'upsert': True}},
                     {'index': 5,
                      'code': 11000,
                      'errmsg': '...',
                      'op': {'_id': '...', 'b': 6, 'a': 1}}]},
                result)
        finally:
            await test_coll.drop_index([('a', 1)])

    @pytest.mark.asyncio
    async def test_large_inserts_ordered(self, mongo, test_coll):
        connection = await mongo.get_connection()
        big = 'x' * connection.max_bson_size
        batch = test_coll.initialize_ordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.insert({'big': big})
        batch.insert({'b': 2, 'a': 2})

        try:
            await batch.execute()
        except BulkWriteError as exc:
            result = exc.details
            assert exc.code == 65
        else:
            pytest.fail('Error not raised')

        assert 1 == result['nInserted']

        await test_coll.delete_many({})

        big = 'x' * (1024 * 1024 * 4)
        batch = test_coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1, 'big': big})
        batch.insert({'a': 2, 'big': big})
        batch.insert({'a': 3, 'big': big})
        batch.insert({'a': 4, 'big': big})
        batch.insert({'a': 5, 'big': big})
        batch.insert({'a': 6, 'big': big})
        result = await batch.execute()

        assert 6 == result['nInserted']
        assert 6 == await test_coll.count()

    @pytest.mark.asyncio
    async def test_large_inserts_unordered(self, mongo, test_coll):
        connection = await mongo.get_connection()
        big = 'x' * connection.max_bson_size
        batch = test_coll.initialize_unordered_bulk_op()
        batch.insert({'b': 1, 'a': 1})
        batch.insert({'big': big})
        batch.insert({'b': 2, 'a': 2})

        try:
            await batch.execute()
        except BulkWriteError as exc:
            result = exc.details
            assert exc.code == 65
        else:
            pytest.fail('Error not raised')

        assert 2 == result['nInserted']

        await test_coll.delete_many({})

        big = 'x' * (1024 * 1024 * 4)
        batch = test_coll.initialize_ordered_bulk_op()
        batch.insert({'a': 1, 'big': big})
        batch.insert({'a': 2, 'big': big})
        batch.insert({'a': 3, 'big': big})
        batch.insert({'a': 4, 'big': big})
        batch.insert({'a': 5, 'big': big})
        batch.insert({'a': 6, 'big': big})
        result = await batch.execute()

        assert 6 == result['nInserted']
        assert 6 == await test_coll.count()

    @pytest.mark.asyncio
    async def test_numerous_inserts(self, test_coll):
        # Ensure we don't exceed server's 1000-document batch size limit.
        n_docs = 2100
        batch = test_coll.initialize_unordered_bulk_op()
        for _ in range(n_docs):
            batch.insert({})

        result = await batch.execute()
        assert n_docs == result['nInserted']
        assert n_docs == await test_coll.count()

        # Same with ordered bulk.
        await test_coll.delete_many({})
        batch = test_coll.initialize_ordered_bulk_op()
        for _ in range(n_docs):
            batch.insert({})

        result = await batch.execute()
        assert n_docs == result['nInserted']
        assert n_docs == await test_coll.count()

    @pytest.mark.asyncio
    async def test_multiple_execution(self, test_coll):
        batch = test_coll.initialize_ordered_bulk_op()
        batch.insert({})
        await batch.execute()
        with pytest.raises(InvalidOperation):
            await batch.execute()

    @pytest.mark.asyncio
    async def test_generator_insert(self, test_coll):
        def gen():
            yield {'a': 1, 'b': 1}
            yield {'a': 1, 'b': 2}
            yield {'a': 2, 'b': 3}
            yield {'a': 3, 'b': 5}
            yield {'a': 5, 'b': 8}

        result = await test_coll.insert_many(gen())
        assert 5 == len(result.inserted_ids)
