import sqlite3
import json
from nss_handler import status
from repository import db_get_single, db_get_all, db_delete, db_update, db_create


class ShippingShipsView():

    def get(self, handler, url):
        if url["pk"] != 0:
            if url["query_params"] == {"_expand": ['hauler']}:
                sql = """SELECT
                        s.id, 
                        s.name, 
                        s.hauler_id,
                        h.id haulerId,
                        h.name haulerName,
                        h.dock_id
                    FROM Ship s 
                    JOIN Hauler h 
                    ON s.hauler_id = h.id
                    WHERE s.hauler_id = ? """
                query_results = db_get_single(sql, url["pk"])
                if query_results:
                    hauler = {
                        "id": query_results['haulerId'],
                        "name": query_results['haulerName'],
                        "dock_id": query_results["dock_id"]
                    }
                    ship = {
                        "id": query_results['id'],
                        "name": query_results['name'],
                        "hauler_id": query_results["hauler_id"],
                        "hauler": hauler
                    }
                    serialized_hauler = json.dumps(ship)

            else:
                sql = """SELECT * FROM Ship s WHERE s.id = ?"""
                query_results = db_get_single(sql, url["pk"])
                if query_results:
                    ship = dict(query_results)
                    serialized_hauler = json.dumps(ship)
            return handler.response(serialized_hauler, status.HTTP_200_SUCCESS.value)
        else:
            sql = """SELECT
                        s.id, 
                        s.name, 
                        s.hauler_id,
                        h.id haulerId,
                        h.name haulerName,
                        h.dock_id
                    FROM Ship s 
                    JOIN Hauler h 
                    ON s.hauler_id = h.id
                    """
            if url["query_params"] == {"_expand": ['hauler']}:
                query_results = db_get_all(sql)
                ships = []
                for row in query_results:
                    hauler = {
                        "id": row['haulerId'],
                        "name": row['haulerName'],
                        "dock_id": row["dock_id"]
                    }
                    ship = {
                        "id": row['id'],
                        "name": row['name'],
                        "hauler_id": row["hauler_id"],
                        "hauler": hauler
                    }
                    ships.append(ship)
                serialized_haulers = json.dumps(ships)

            else:
                sql = """SELECT * FROM Ship"""
                query_results = db_get_all(sql)
                ships = []
                for row in query_results:
                    ship = dict(row)
                    ships.append(ship)
                serialized_haulers = json.dumps(ships)
            return handler.response(serialized_haulers, status.HTTP_200_SUCCESS.value)

    def delete(self, handler, pk):
        number_of_rows_deleted = db_delete("DELETE FROM Ship WHERE id = ?", pk)

        if number_of_rows_deleted > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response("", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value)

    def update(self, handler, ship_data, pk):
        sql = """
        UPDATE Ship
        SET
            name = ?,
            hauler_id = ?
        WHERE id = ?
        """
        number_of_rows_updated = db_update(
            sql,
            (ship_data['name'], ship_data['hauler_id'], pk)
        )

        if number_of_rows_updated > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response("", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value)

    def create(self, handler, ship_data):
        sql = """
        INSERT INTO Ship Values(null, ?, ?)
        """
        number_of_rows_created = db_create(
            sql,
            (ship_data['name'], ship_data['hauler_id'])
        )

        if number_of_rows_created > 0:
            return handler.response("", status.HTTP_201_SUCCESS_CREATED.value)
        else:
            return handler.response("", status.HTTP_400_CLIENT_ERROR_BAD_REQUEST_DATA.value)
