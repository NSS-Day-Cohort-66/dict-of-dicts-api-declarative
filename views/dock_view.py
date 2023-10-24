import json
from nss_handler import status
from repository import db_get_single, db_get_all, db_delete, db_update, db_create

class DocksView():

    def get(self, handler, url):
        if url["pk"] != 0:
            sql = """
            SELECT
                d.id,
                d.location,
                d.capacity
            FROM Dock d
            WHERE d.id = ?
            """
            query_results = db_get_single(sql, url["pk"])
            serialized_hauler = json.dumps(dict(query_results))

            return handler.response(serialized_hauler, status.HTTP_200_SUCCESS.value)
        else:

            query_results = db_get_all(sql = """
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
            docks = {} #! initializes the empty dictionary
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
            serialized_haulers = json.dumps(list(docks.values()))

            return handler.response(serialized_haulers, status.HTTP_200_SUCCESS.value)

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