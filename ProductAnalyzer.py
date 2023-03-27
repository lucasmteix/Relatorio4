from main import db


class ProductAnalyzer:

    def totalGastoClienteB(self):

        result = db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group":
                 {"_id": "B", "total":
                     {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}
                  }
            },
            {"$group": {"_id": None, "total gasto": "$total"}}
        ])

        return result

    def produtoMenosVendido(self):

        result = db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$produtos.nome", "total": {"$sum": "$produtos.quantidade"}}},
            {"$sort": {"total": 1}},
            {"$limit": 1}
        ])

        return result

    def clienteGastouMenos1Compra(self):
        result = db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": {"idCompra": "$_id"},
                        "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
            {"$sort": {"total": 1}},
            {"$limit": 1}
        ])

        return result

    def produtosQuantVendMaior2(self):
        result = db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$produtos.nome", "total": {"$sum": "$produtos.quantidade"}}},
            {"$group": {"_id": None, "$quantidade vendida": {"$gte": 2}}}
        ])

        return result