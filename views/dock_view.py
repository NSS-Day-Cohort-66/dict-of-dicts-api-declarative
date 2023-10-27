import json
from nss_handler import status
from repository import db_get_single, db_get_all, db_delete, db_update, db_create, db_get_multiple


class DocksView():

    def get(self, handler, url):
        if url["pk"] != 0:
            if url['query_params'] == {'_embed': ['hauler']}:
                sql = """
                SELECT
                                d.id dockId, 
                                d.location, 
                                d.capacity,
                                h.id,
                                h.name,
                                h.dock_id
                            FROM Dock d 
                            JOIN Hauler h 
                            ON d.id = h.dock_id
                            WHERE d.id = ?
                """
                query_results = db_get_multiple(sql, url["pk"])
                dock = {}
                for row in query_results:
                    dock_id = row["dockId"]
                    if dock_id not in dock:
                        dock[dock_id] = {
                            "id": row['dockId'],
                            "location": row['location'],
                            "capacity": row["capacity"],
                            "haulers": []
                        }
                    hauler = {
                        "id": row['id'],
                        "name": row['name'],
                        "dock_id": row["dock_id"],
                    }
                    dock[dock_id]["haulers"].append(hauler)
                serialized_dock = json.dumps(list(dock.values()))
            else:
                query_results = db_get_single(
                    """SELECT * FROM Dock d WHERE d.id = ?""", url["pk"])
                dock = dict(query_results)
                serialized_dock = json.dumps(dock)
            return handler.response(serialized_dock, status.HTTP_200_SUCCESS.value)
        else:
            if url['query_params'] == {'_embed': ['hauler']}:
                query_results = db_get_all(
                    sql="""
                        SELECT
                                d.id dockId, 
                                d.location, 
                                d.capacity,
                                h.id,
                                h.name,
                                h.dock_id
                            FROM Dock d 
                            JOIN Hauler h 
                            ON d.id = h.dock_id
                """)
                docks = {}  # ! initializes the empty dictionary
                for row in query_results:
                    dock_id = row["dockId"]
                    if dock_id not in docks:
                        docks[dock_id] = {
                            "id": row['dockId'],
                            "location": row['location'],
                            "capacity": row["capacity"],
                            "haulers": []
                        }
                    hauler = {
                        "id": row['id'],
                        "name": row['name'],
                        "dock_id": row["dock_id"],
                    }
                    docks[dock_id]["haulers"].append(hauler)
                serialized_docks = json.dumps(list(docks.values()))

            else:
                query_results = db_get_all(sql="""SELECT * FROM Dock""")
                docks = []
                for row in query_results:
                    dock = dict(row)
                    docks.append(dock)
                serialized_docks = json.dumps(docks)
            return handler.response(serialized_docks, status.HTTP_200_SUCCESS.value)

    def delete(self, handler, pk):
        number_of_rows_deleted = db_delete("DELETE FROM Dock WHERE id = ?", pk)

        if number_of_rows_deleted > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response("", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value)

    def update(self, handler, dock_data, pk):
        sql = """
        UPDATE Dock
        SET
            location = ?,
            capacity = ?
        WHERE id = ?
        """
        number_of_rows_updated = db_update(
            sql,
            (dock_data['location'], dock_data['capacity'], pk)
        )

        if number_of_rows_updated > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response("", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value)

    def create(self, handler, dock_data):
        sql = """
        INSERT INTO Dock Values(null, ?, ?)
        """
        number_of_rows_created = db_create(
            sql,
            (dock_data['location'], dock_data['capacity'])
        )

        if number_of_rows_created > 0:
            return handler.response("", status.HTTP_201_SUCCESS_CREATED.value)
        else:
            return handler.response("", status.HTTP_400_CLIENT_ERROR_BAD_REQUEST_DATA.value)
