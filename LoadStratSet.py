from neo4j import GraphDatabase as gd
import dbcfg
import sys

class LoadStratSet():
    def __init__(self, flnm):
        self._flnm = flnm
        self._drv = None
        self._sess = None

    def connect(self):
        self._drv = gd.driver(dbcfg.uri, auth=(dbcfg.usr, dbcfg.pw))
        self._sess = self._drv.session(database=dbcfg.db)

    def disconnect(self):
        self._drv.close()

    def stdweight(self, strats):
        # We only want a max of 6 "heavy" weights, with the rest being 1.
        # So 9 would be 6 5 4 3 2 1 1 1 1 and
        #    7 would be 6 5 4 3 2 1 1 and
        #    4 would be 4 3 2 1.
        stratcnt = len(strats.split("+"))
        maxwgt = min(5, stratcnt)
        weights = [max(r, 1) for r in range(maxwgt, maxwgt - stratcnt, -1)]
        return ("".join([str(w) for w in weights]))

    def load2neo(self, clearOut=True):
        # connect to Neo4j
        # get data from file
        # load data to Neo
        # disconnect
        self.connect()
        if clearOut:
            clr = "MATCH (x) DETACH DELETE x"
            self._sess.run(clr)
            print("Mass delete complete (maybe)")
        savflnm = self._flnm.split("\\")[-1]
        # create the source file name
        cr1 = """
        CREATE (:SourceFile {{filename: '{0}'}})
        """.format(savflnm)
        print(cr1)
        self._sess.run(cr1)
        fl = open(self._flnm)
        for ln in fl:
            flds = ln.strip().split(":")
            if len(flds) >= 7:
                (strats, winrt, cnt, z1, z1, z3, wgt) = flds
            else:
                strats = flds[0]
                wgt = self.stdweight(strats)
            cr2 = """
            MATCH (sf:SourceFile {{filename: '{0}'}})
            CREATE (ss:StrategySet {{strategies: '{1}', weight: {2}}})-[:FROM]->(sf)
            """.format(savflnm, strats, wgt)
            print(cr2)
            self._sess.run(cr2)
        fl.close()
        self.disconnect()

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print("usage: {0} strategySetFile".format(sys.argv[0]))
        sys.exit(-1)

    lss = LoadStratSet(sys.argv[1])
    lss.load2neo()
