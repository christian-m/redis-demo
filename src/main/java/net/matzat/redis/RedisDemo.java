package net.matzat.redis;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Autocomplete Demo
 * <p/>
 * Basiert auf der Ruby-Demo von Wolfgang Werner / picturesafe GmbH / 03-2012
 * <p/>
 * Inspiriert von http://antirez.com/post/autocomplete-with-redis.html
 * Inspiriert von http://patshaughnessy.net/2011/11/29/two-ways-of-using-redis-to-build-a-nosql-autocomplete-search-index
 *
 * @author Christian Matzat <christian@matzat.net>
 */
public class RedisDemo {

    private static final Logger LOG = Logger.getLogger(RedisDemo.class.getSimpleName());

    // Public Constructor deaktivieren
    private RedisDemo() {}

    /**
     * Der Anwendung können zwei Parameter in beliebiger Reihenfolge übergeben werden:
     * - "clean" um bestehende Daten vor dem Import zu löschen
     * - Ein Dateiname, dessen Zeilen als Werte in die Redis-DB importiert werden sollen
     * 
     * @param args Befehlszeilenparameter
     */
    public static void main(String... args) {
        RedisDemo redisDemo = new RedisDemo();
        RedisConnector connector = null;
        Jedis jedis = null;
        try {
            connector = new RedisConnector();
            jedis = connector.startSession();
            jedis.select(0);
            String filename = parseParameters(redisDemo, connector, jedis, args);
            if (filename != null) {
                redisDemo.fillData(connector, jedis, filename);
            }
            redisDemo.show(connector, jedis, "Otto");
            redisDemo.show(connector, jedis, "otto");
            redisDemo.show(connector, jedis, "Wolf");
            redisDemo.show(connector, jedis, "wang");
            redisDemo.show(connector, jedis, "Ot", "Be");
            redisDemo.show(connector, jedis, "Wo", "Ma");
            redisDemo.show(connector, jedis, "Wol", "Bau");
            redisDemo.show(connector, jedis, "zi", "li");
            redisDemo.show(connector, jedis, "zi", "di", "li");
        } finally {
            if (connector != null) {
                if (jedis != null) {
                    connector.stopSession(jedis);
                }
                connector.destroy();
                connector = null;
            }
        }
    }

    private static String parseParameters(RedisDemo redisDemo, RedisConnector connector, Jedis jedis, String[] args) {
        String filename = null;
        boolean cleanPerformed = false;
        if (args != null && args.length > 0) {
            if (args[0].equalsIgnoreCase("clean")) {
                redisDemo.clean(connector, jedis);
                cleanPerformed = true;
            } else {
                filename = args[0];
            }
            if (args.length > 1) {
                if (args[1].equalsIgnoreCase("clean")) {
                    if (!cleanPerformed) {
                        redisDemo.clean(connector, jedis);
                        cleanPerformed = true;
                    }
                } else {
                    filename = args[1];
                }
            }
        }
        return filename;
    }

    private void clean(RedisConnector connector, Jedis jedis) {
        LOG.info("Löschen der Datensätze");
        LOG.info("---------------------------------------------------------");
        long timeStart = System.nanoTime();
        connector.clean(jedis);
        long timeEnd = System.nanoTime();
        LOG.info(String.format("Time elapsed %fms", new Float(timeEnd - timeStart) / 1000000));
        LOG.info("---------------------------------------------------------");
    }

    private void fillData(RedisConnector connector, Jedis jedis, String filename) {
        LOG.info("Initiales füllen der Datenbank");
        LOG.info("---------------------------------------------------------");
        long timeStart = System.nanoTime();
        connector.fillData(jedis, filename);
        long timeEnd = System.nanoTime();
        LOG.info(String.format("Time elapsed %fms", new Float(timeEnd - timeStart) / 1000000));
        LOG.info("---------------------------------------------------------");
    }

    private void show(RedisConnector connector, Jedis jedis, String... terms) {
        StringBuffer searchTerms = new StringBuffer();
        boolean isFirst = true;
        for (String term : terms) {
            if (!isFirst) {
                searchTerms.append(", ");
            }
            isFirst = false;
            searchTerms.append("'").append(term).append("'");
        }
        LOG.info(String.format("Suche nach %s...", searchTerms));
        LOG.info("---------------------------------------------------------");
        long timeStart = System.nanoTime();
        List<String> result = connector.complete(jedis, terms);
        long timeEnd = System.nanoTime();
        LOG.info(String.format("Time elapsed %fms", new Float(timeEnd - timeStart) / 1000000));
        if (result != null) {
            for (String entry : result) {
                LOG.info(entry);
            }
        }
        LOG.info("---------------------------------------------------------");
    }

}
