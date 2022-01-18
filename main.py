import atexit
import sys
import sqlite3

# in order to run the program provide the following arguments:
# config.txt orders.txt output
# config.txt provides the hats in stock and the suppliers selling them, orders.txt provides the orders requested
# the program creates a data base named database.db and writes to the output.txt file


def main():
    # parse config file
    repo = _Repository()
    repo.create_tables()
    args = sys.argv[1:]
    with open(args[0]) as f:
        first_line = f.readline()
        first, second = first_line.split(",")
        for i in range(int(first)):
            line = f.readline()
            hat_id, hat_topping, hat_supplier, hat_quantity = line.split(",")
            hat_quantity_string = str(hat_quantity)  # this is done in order to remove the \n char added to the
            # last string in the line
            if hat_quantity[-1] == '\n':
                hat_quantity_string = hat_quantity_string[:-1]
            hat_dto = Hat(hat_id, hat_topping, hat_supplier, hat_quantity_string)
            repo.hats.insert(hat_dto)
        for i in range(int(second)):
            line = f.readline()
            supplier_id, supplier_name = line.split(",")
            supplier_name_string = str(supplier_name)
            if supplier_name[-1] == '\n':
                supplier_name_string = supplier_name_string[:-1]
            supplier_dto = Supplier(supplier_id, supplier_name_string)
            repo.suppliers.insert(supplier_dto)

    # parse orders file
    cursor = repo.conn.cursor()
    order_id = 1
    with open(args[1]) as r:
        with open(args[2], 'w') as w:
            for line in r:
                if line == '\n':
                    break
                else:
                    location, topping = line.split(",")
                    topping_str = str(topping)
                    if topping_str[-1] == '\n':
                        topping_str = topping_str[:-1]
                    hat_object = repo.hats.find(topping_str)
                    if hat_object is not None:
                        supplier_object = repo.suppliers.find(hat_object.supplier)
                        order_dto = Order(order_id, location, hat_object.id)
                        repo.orders.insert(order_dto)
                        repo.hats.update(hat_object)
                        string = topping_str + ',' + supplier_object.name + ',' + location + '\n'
                        w.write(string)
                        order_id = order_id + 1

            w.close()
    repo.close()


# The Repository
class _Repository:
    def __init__(self):
        self._conn = sqlite3.connect('database.db')
        self.hats = Hats(self._conn)
        self.suppliers = Suppliers(self._conn)
        self.orders = Orders(self._conn)

    def close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
       CREATE TABLE hats (
           id          INT         PRIMARY KEY,
           topping     TEXT        NOT NULL,
           supplier    INT         NOT NULL,
           quantity    INT         NOT NULL,

           FOREIGN KEY(supplier)     REFERENCES suppliers(id)
       );

       CREATE TABLE suppliers (
           id       INT     PRIMARY KEY,
           name     TEXT    NOT NULL
       );

       CREATE TABLE orders (
           id            INT     PRIMARY KEY,
           location      TEXT    NOT NULL,
           hat           INT     NOT NULL,

           FOREIGN KEY(hat)     REFERENCES hats(id)

       );
   """)

    @property
    def conn(self):
        return self._conn


# DTO
class Hat:
    def __init__(self, id, topping, supplier, quantity):
        self.id = id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity


class Supplier:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Order:
    def __init__(self, id, location, hat):
        self.id = id
        self.location = location
        self.hat = hat


# DAO
class Hats:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, hat):
        self._conn.execute("""
               INSERT INTO Hats (id, topping, supplier, quantity) VALUES (?, ?, ?, ?)
           """, [hat.id, hat.topping, hat.supplier, hat.quantity])

    def find(self, hats_topping):
        c = self._conn.cursor()
        c.execute("""
            SELECT id, topping, supplier, quantity FROM hats WHERE topping = ?
        """, [hats_topping])
        rows = c.fetchall()
        hatArr = []
        for row in rows:
            hatArr.append(Hat(*row))
        index = 0
        for i in range(len(hatArr)):
            if hatArr[i].supplier < hatArr[index].supplier:
                index = i
        return hatArr[index]

    def update(self, hat):
        self._conn.execute("""
               UPDATE hats SET quantity=(?) WHERE id=(?) 
           """, [hat.quantity - 1, hat.id])
        if hat.quantity == 1:
            self.delete(hat)

    def delete(self, hat):
        self._conn.execute("""
            DELETE FROM hats WHERE id=(?)
        """, [hat.id])

    def print_all(self):
        c = self._conn.cursor()
        all = c.execute("""
            SELECT * FROM hats
        """).fetchall()
        print(all)


class Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("""
                INSERT INTO suppliers (id, name) VALUES (?, ?)
        """, [supplier.id, supplier.name])

    def find(self, supplier_id):
        c = self._conn.cursor()
        c.execute("""
                SELECT *
                 FROM suppliers WHERE id = ?
            """, [supplier_id])

        return Supplier(*c.fetchone())


class Orders:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, order):
        self._conn.execute("""
            INSERT INTO orders (id, location, hat) VALUES (?, ?, ?)
        """, [order.id, order.location, order.hat])


main()
