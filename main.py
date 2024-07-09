import mysql.connector
import logging
import handler
import schema

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        # Establish connection to MySQL
        connection = mysql.connector.connect(user="root",
                                             password="P@ssw0rd",
                                             host="localhost",
                                             database="fakestore")
        if connection.is_connected():
            cursor = connection.cursor()
            logger.info("Database successfully connected")

            # Create tables
            schema.create_products_table(conn=connection, cursor=cursor)
            schema.create_carts_table(conn=connection, cursor=cursor)
            schema.create_user_table(conn=connection, cursor=cursor)

            # Insert product data
            product_data = handler.fetch_data('products')  # Fetch data from API
            for product in product_data:
                flatten_dict = handler.flatten_product(prod=product)  # Flatten each dictionary
                schema.insert_into_products(data=flatten_dict, conn=connection, cursor=cursor)

            # Insert cart data
            cart_data = handler.fetch_data('carts')  # Fetch data from API
            for cart in cart_data:
                flatten_cart = handler.flatten_cart(cart)  # Flatten each dictionary
                for cart_item in flatten_cart:
                    schema.insert_into_carts(data=cart_item, conn=connection, cursor=cursor)

            # Insert user data
            user_data = handler.fetch_data('users')  # Fetch data from API
            for user in user_data:
                flatten_user = handler.flatten_user(user)  # Flatten each dictionary
                schema.insert_into_users(data=flatten_user, conn=connection, cursor=cursor)
        
    except mysql.connector.Error as err:
        logger.error(f"Error occurred while connecting to MySQL: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("MySQL connection closed")
