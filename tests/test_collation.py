from unittest import mock

import pytest
from pymongo import message
from pymongo.collation import Collation


class TestCollation:

    collation = Collation('en_US')

    @pytest.mark.asyncio
    async def test_create_collection(self, test_db):
        await test_db.test.drop()
        await test_db.create_collection('test', collation=self.collation)

        await test_db.test.drop()
        await test_db.create_collection('test', collation=self.collation.document)

    @pytest.mark.asyncio
    async def test_create_index(self, test_db):
        await test_db.test.create_index('foo', collation=self.collation)
        result = await test_db.test.index_information()
        index = result['foo_1']
        assert index['collation']['locale'] == self.collation.document['locale']

    @pytest.mark.asyncio
    async def test_aggregate(self, test_db):
        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            await test_db.test.aggregate(
                [{'$group': {'_id': 42}}], collation=self.collation)

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

    @pytest.mark.asyncio
    async def test_count(self, test_db):
        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            await test_db.test.count(collation=self.collation)

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            await test_db.test.find(collation=self.collation).count()

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

    @pytest.mark.asyncio
    async def test_distinct(self, test_db):
        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:

            await test_db.test.distinct('foo', collation=self.collation)

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:

            await test_db.test.find(collation=self.collation).distinct('foo')

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

    @pytest.mark.asyncio
    async def test_find_command(self, test_db):
        await test_db.test.insert_one({'is this thing on?': True})

        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            async for _ in test_db.test.find(collation=self.collation):
                pass

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

    @pytest.mark.asyncio
    async def test_group(self, test_db):
        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            await test_db.test.group(
                'foo', {'foo': {'$gt': 42}}, {}, 'function(a, b) { return a; }',
                collation=self.collation
            )

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

    @pytest.mark.asyncio
    async def test_map_reduce(self, test_db):
        await test_db.create_collection('test', collation=self.collation)
        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            await test_db.test.map_reduce(
                'function() {}', 'function() {}', 'output',
                collation=self.collation
            )

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

    @pytest.mark.asyncio
    async def test_delete(self, test_db):
        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            await test_db.test.delete_one({'foo': 42}, collation=self.collation)
            assert mock_query.called
            query_args, _ = mock_query.call_args

            for delete_query in query_args[4]['deletes']:
                assert delete_query['collation'] == self.collation.document

        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            await test_db.test.delete_many({'foo': 42}, collation=self.collation)
            assert mock_query.called
            query_args, _ = mock_query.call_args

            for delete_query in query_args[4]['deletes']:
                assert delete_query['collation'] == self.collation.document

    @pytest.mark.asyncio
    async def test_update(self, test_db):
        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            await test_db.test.replace_one({'foo': 42}, {'foo': 43},
                                           collation=self.collation)

            assert mock_query.called
            query_args, _ = mock_query.call_args
            for delete_query in query_args[4]['updates']:
                assert delete_query['collation'] == self.collation.document

        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            await test_db.test.update_one({'foo': 42}, {'$set': {'foo': 43}},
                                          collation=self.collation)

            assert mock_query.called
            query_args, _ = mock_query.call_args
            for delete_query in query_args[4]['updates']:
                assert delete_query['collation'] == self.collation.document

        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            await test_db.test.update_many({'foo': 42}, {'$set': {'foo': 43}},
                                           collation=self.collation)

            assert mock_query.called
            query_args, _ = mock_query.call_args
            for delete_query in query_args[4]['updates']:
                assert delete_query['collation'] == self.collation.document

    @pytest.mark.asyncio
    async def test_find_and(self, test_db):
        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:

            await test_db.test.find_one_and_delete({'foo': 42}, collation=self.collation)

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:

            await test_db.test.find_one_and_update({'foo': 42}, {'$set': {'foo': 43}},
                                                   collation=self.collation)

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:

            await test_db.test.find_one_and_replace({'foo': 42}, {'foo': 43},
                                                    collation=self.collation)

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document

    @pytest.mark.asyncio
    async def test_bulk(self, test_db):

        bulk = test_db.test.initialize_ordered_bulk_op()
        with mock.patch.object(bulk._BulkOperationBuilder__bulk, '_do_batched_write_command', wraps=bulk._BulkOperationBuilder__bulk._do_batched_write_command) as mock_query:
            bulk.find({'noCollation': 42}).remove_one()
            bulk.find({'noCollation': 42}).remove()
            bulk.find({'foo': 42}, collation=self.collation).remove_one()
            bulk.find({'foo': 42}, collation=self.collation).remove()
            bulk.find({'noCollation': 24}).replace_one({'bar': 42})
            bulk.find({'noCollation': 84}).upsert().update_one(
                {'$set': {'foo': 10}})
            bulk.find({'noCollation': 45}).update({'$set': {'bar': 42}})
            bulk.find({'foo': 24}, collation=self.collation).replace_one(
                {'foo': 42})
            bulk.find({'foo': 84}, collation=self.collation).upsert().update_one(
                {'$set': {'foo': 10}})
            bulk.find({'foo': 45}, collation=self.collation).update({
                '$set': {'foo': 42}})
            await bulk.execute()

            assert mock_query.called
            query_args, _ = mock_query.call_args

            for op in query_args[3]:
                if 'noCollation' in op['q']:
                    assert 'collation' not in op
                else:
                    assert self.collation.document == op['collation']

    @pytest.mark.asyncio
    async def test_cursor_collation(self, test_db):
        await test_db.test.insert_one({'hello': 'world'})

        with mock.patch('pymongo.message.query', wraps=message.query) as mock_query:
            items = []
            async for item in test_db.test.find().collation(self.collation):
                items.append(item)

            assert len(items) > 0

            assert mock_query.called
            query_args, _ = mock_query.call_args
            assert query_args[4]['collation'] == self.collation.document



