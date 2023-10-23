import json
from nss_handler import status
from repository import db_get_single, db_get_all, db_delete, db_update, db_create


class HaulerView():

    def get(self, handler, pk):
        if pk != 0:
            sql = """SELECT
                        h.id, 
                        h.name, 
                        h.dock_id,
                        d.id,
                        d.location,
                        d.capacity
                    FROM Hauler h 
                    JOIN Dock d 
                    ON h.dock_id = d.id
                    WHERE h.id = ? """
            query_results = db_get_single(sql, pk)
            if query_results:
                hauler = dict(query_results)
                dock_info = {
                    "id": hauler["dock_id"],
                    "location": hauler["location"],
                    "capacity": hauler["capacity"]
                }
                hauler["dock"] = dock_info
                
                del hauler["location"]
                del hauler["capacity"]
                serialized_hauler = json.dumps(hauler)
            

            return handler.response(serialized_hauler, status.HTTP_200_SUCCESS.value)
        else:

            sql = """SELECT
                        h.id, 
                        h.name, 
                        h.dock_id,
                        d.id,
                        d.location,
                        d.capacity
                    FROM Hauler h 
                    JOIN Dock d 
                    ON h.dock_id = d.id
                    """
            query_results = db_get_all(sql)
            haulers = []
            for row in query_results:
                dock = {
                    "id": row["dock_id"],
                    "location": row["location"],
                    "capacity": row["capacity"]
                }
                hauler = {
                    "id": row['id'],
                    "name": row['name'],
                    "dock_id": row["dock_id"],
                    "dock": dock
                }
                haulers.append(hauler)
            serialized_haulers = json.dumps(haulers)

            return handler.response(serialized_haulers, status.HTTP_200_SUCCESS.value)

    def delete(self, handler, pk):
        number_of_rows_deleted = db_delete(
            "DELETE FROM Hauler WHERE id = ?", pk)

        if number_of_rows_deleted > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response("", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value)

    def update(self, handler, hauler_data, pk):
        sql = """
        UPDATE Hauler
        SET
            name = ?,
            dock_id = ?
        WHERE id = ?
        """
        number_of_rows_updated = db_update(
            sql,
            (hauler_data['name'], hauler_data['dock_id'], pk)
        )

        if number_of_rows_updated > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response("", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value)

    def create(self, handler, hauler_data):
        sql = """
        INSERT INTO Hauler Values(null, ?, ?)
        """
        number_of_rows_created = db_create(
            sql,
            (hauler_data['name'], hauler_data['dock_id'])
        )

        if number_of_rows_created > 0:
            return handler.response("", status.HTTP_201_SUCCESS_CREATED.value)
        else:
            return handler.response("", status.HTTP_400_CLIENT_ERROR_BAD_REQUEST_DATA.value)
