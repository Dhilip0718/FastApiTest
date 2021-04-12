from typing import Optional
from fastapi import FastAPI,Header, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.structure.io import graphsonV3d0
from gremlin_python.process.graph_traversal import __,union, values, constant, unfold
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver import client,resultset
from gremlin_python.process.traversal import T, P, Operator, Order, neq
from typing import Optional
from pydantic import BaseModel
import datetime
import uuid
import json

statics.load_statics(globals())

conn = client.Client('ws://10.1.0.4:8182/gremlin','g')



# endpoint = 'ws://52.174.65.201:8182/gremlin'
endpoint = 'ws://10.1.0.4:8182/gremlin'

graph = Graph()

connection = DriverRemoteConnection(endpoint,'g')
# The connection should be closed on shut down to close open connections with connection.close()
g = graph.traversal().withRemote(connection)
# Reuse 'g' across the application

app = FastAPI()



@app.get("/")
def read_root():
    return {"Hello": "Word"}

@app.get("/products")
def read_products(token: Optional[str] = Header(None)):
    #writer = graphsonV3d0.GraphSONWriter() 
    #products = writer.toDict(g.V().hasLabel('Product').limit(2).project('Product Id','Product Name').by('productID').by('productName').toList())
    #products = writer.writeObject(g.V().hasLabel('Product').limit(2).project('Product Id','Product Name').by('productID').by('productName').toList())
    products = g.V().hasLabel('Product').limit(2).project('Product Id','Product Name','supplierID','categoryID','discontinued').\
                by('productID').by('productName').by('supplierID').by('categoryID').by('discontinued').toList()
    #pjson = json.dumps(products)
    return products

@app.get("/products/{id}")
def read_products(request: Request, id: str):
    """Returns product details based on the ProductID"""
    try:
        ret = {}
        product = g.V().has('Product','productID',id).project('Product Id','Product Name').by('productID').by('productName').toList()
        return product
    except Exception as e:
        return str(e)

@app.get("/suppliers")
def read_suppliers(token: Optional[str] = Header(None)):
    """Returns product details based on the Supplier"""
    try:
        supplier = g.V().hasLabel('Supplier').valueMap().toList()
        return supplier
    except Exception as e:
        return str(e)

@app.get("/product-categories")
def read_categories(token: Optional[str] = Header(None)):
    """ Returns categories """
    try:
        category = g.V().hasLabel('Category').valueMap().toList()   
        return category
    except Exception as e:
        return str(e)


@app.get("/all-product-cat-supplier")
def read_all_products_with_category_supplier(token: Optional[str] = Header(None)):
    """Returns all product catogery and supplier details"""
    try:
        pcs = g.V().hasLabel("Product").match(as_("c").values("productID").as_("Product ID"),\
                as_("c").values("productName").as_("Product Name"),\
                as_("c").out("PART_OF").values("categoryID").as_("Category ID"),\
                as_("c").out("PART_OF").values("categoryName").as_("Category Name"),\
                as_("c").in_("SUPPLIES").values("supplierID").as_("Supplier ID"),\
                as_("c").in_("SUPPLIES").values("companyName").as_("Company Name"),\
                ).select("Product ID","Product Name","Category ID","Category Name","Supplier ID","Company Name").toList()
        return pcs
    
    except Exception as e:
        return str(e)

@app.get("/filter-products")
def read_filter_products(productId: int = 0, categoryId: int = 0, supplierId: int = 0):
    """Returns filter result based on productId, categoryId or supplierId, empty if no match"""
    # check supplied product id. if it is not 0 then filter on product
    if productId == 0:
       
        product_query = 'g.V().hasLabel("Product")'
                
    else:
        product_query = 'g.V().has("Product","productID","{}")'.format(productId)


    # check supplied category id. if it is not 0 then filter on category   
    if categoryId == 0:
       
        category_query = 'hasLabel("Category")'
            
    else:
        category_query = 'has("Category","categoryID","{}")'.format(categoryId)

    
    # check supplied supplier id. if it is not 0 then filter on supplier
    if supplierId == 0:
       
        supplier_query = 'hasLabel("Supplier")'
            
    else:
        supplier_query = 'has("Supplier","supplierID","{}")'.format(supplierId)
    
    
    finalquery = '{}.match(__.as("c").values("productID").as("Product ID"),\
                __.as("c").values("productName").as("Product Name"),\
                __.as("c").out("PART_OF").{}.values("categoryID").as("Category ID"),\
                __.as("c").out("PART_OF").{}.values("categoryName").as("Category Name"),\
                __.as("c").in("SUPPLIES").{}.values("supplierID").as("Supplier ID"),\
                __.as("c").in("SUPPLIES").{}.values("companyName").as("Company Name"),\
                ).select("Product ID","Product Name","Category ID","Category Name","Supplier ID","Company Name").toList()'.format(product_query,category_query,category_query,supplier_query,supplier_query)
    query_submit = conn.submit(finalquery)
    future_results = query_submit.all()
    results = future_results.result()
    return results
