import logging


def create_db_engine():
    """
    create_db_engine function from dailyDataLoadV4d.py

    what follows is a mock object
    """
    class MockEngine:
        def execute(*args, **kwargs):
            logging.debug("executing query: %s", str(args))
            return [
                {'customer': 'customer1'},
                {'customer': 'customer2'},
                {'customer': 'customer3'},
                {'customer': 'customer4'},
                {'customer': 'customer5'},
            ]
    return MockEngine()


engine = create_db_engine()


def customers():
    customer_query = """
        Select * From customers3 Where ctype = 'CUSTOMER' OR ctype = 'VRBO'
    """
    customer_results = engine.execute(customer_query)
    return {result['customer'] for result in customer_results}
