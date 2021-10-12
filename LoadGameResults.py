from neo4j import GraphDatabase as gd
import dbcfg
import sys

class LoadGameResults():
    def __init__(self, flnm):
        self._flnm = flnm
        self._drv = None
        self._sess = None

    def connect(self):
        self._drv = gd.driver(dbcfg.uri, auth=(dbcfg.usr, dbcfg.pw))
        self._sess = self._drv.session(database=dbcfg.db)

    def disconnect(self):
        self._drv.close()

    def load2neo(self):
        # CREATE CONSTRAINT uniq_game_id ON (g:Game) ASSERT g.gameid IS UNIQUE
        # connect to Neo4j
        # get data from file
        # load data to Neo
        # disconnect
        self.connect()
        savflnm = self._flnm.split("\\")[-1]
        fl = open(self._flnm)
        prevgame = ""
        playpos = -1
        inscnt = 0
        for ln in fl:
            flds = ln.strip().split()
            if len(flds) < 5:
                print("not enough fields " + ln)
                continue
            gameid = flds[0]
            if gameid != prevgame:
                playpos = 1
                prevgame = gameid
            else:
                playpos += 1
            winloss = flds[1][0]
            strats = "+".join(flds[3:-4])
            scor = flds[-1]
            # currently matching only on strategy set, not including weight
            # cr = """
            # MATCH (ss:StrategySet {{strategies: '{0}'}})
            # MERGE (g:Game {{gameid: '{1}'}}) <-[:PLAYS_IN]-
            #     (gp:GamePlayer {{winlossflag: '{2}', score: {3}}}) -[:USES]->
            #     (ss)
            # """.format(strats, gameid, winloss, scor)
            # cr = """
            # MATCH (ss:StrategySet {{strategies: '{0}'}})
            # MERGE (g:Game {{gameid: '{1}'}}) <-[:PLAYS_IN]-
            #     (gp:GamePlayer {{winlossflag: '{2}', score: {3}}})
            # MERGE (gp)-[:USES]->(ss)
            # """.format(strats, gameid, winloss, scor)
            cr = """
            MERGE (ss:StrategySet {{strategies: '{0}'}})
            MERGE (g:Game {{gameid: '{1}', filename: '{4}'}})
            CREATE (gp:GamePlayer {{position: {5}, winlossflag: '{2}', score: {3}}}) 
            CREATE (g)<-[:PLAYS_IN]-(gp)
            CREATE (gp)-[:USES]->(ss)
            """.format(strats, gameid, winloss, scor, savflnm, playpos)
            # print(cr)
            self._sess.run(cr)
            inscnt += 1
            if inscnt % 100 == 0:
                print("{0} game players & {1} games added".format(inscnt, inscnt // 4))
        fl.close()
        print("{0} game players & {1} games added".format(inscnt, inscnt / 4))
        self.disconnect()

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print("usage: {0} gameResultsFile".format(sys.argv[0]))
        sys.exit(-1)

    lss = LoadGameResults(sys.argv[1])
    lss.load2neo()
