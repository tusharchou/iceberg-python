def test_rewrite_position_delete_files_procedure():

    def create_table():
        pass

    def drop_table():
        pass

    def run_sql():
        pass

    def load_table():
        pass

    def deleted_files():
        pass

    def snapshot_summary():
        return []



    def test_expire_delete_files_all():
        catalog = 'db'
        identifier = 'name'
        create_table()
        sqls = [
            f"DELETE FROM {identifier} where id=1",
            f"DELETE FROM {identifier} where id=2",
        ]
        run_sql(sqls)

        table = load_table()

        assert 2 == deleted_files()

        procedure_sql = f"""
            CALL {catalog}.system.rewrite_position_delete_files(
                table=> '{identifier}',
                option=> map( 'rewrite_all', 'true')
            )
        """
        table.refresh()

        snapshot_summary = snapshot_summary()

        assert

        assertEquals(
            "Should delete 2 delete files and add 1",
            ImmutableList.of(
                row(
                    2,
                    1,
                    Long.valueOf(snapshotSummary.get(REMOVED_FILE_SIZE_PROP)),
                    Long.valueOf(snapshotSummary.get(ADDED_FILE_SIZE_PROP)))),
            output);

        assertThat(TestHelpers.deleteFiles(table)).hasSize(1);




