package net.matzat.redis;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Connector Klasse zur Redis-DB
 *
 * @author Christian Matzat <christian@matzat.net>
 */
public class RedisConnector {

    private final static Logger LOG = Logger.getLogger(RedisConnector.class.getSimpleName());

    public JedisPool pool;

    private final static String KEY_PREFIX = "acd";
    private final static String KEY_DOCUMENT_ID = KEY_PREFIX + ".nextid";
    private final static String KEY_DOCUMENTS = KEY_PREFIX + ".dcs";
    private final static String KEY_TERMS_BASE = KEY_PREFIX + ".trm";

    public RedisConnector() {
        pool = new JedisPool(new JedisPoolConfig(), "localhost");
    }

    public void selectDB(Jedis jedis, int index) {
        jedis.select(index);
    }

    /**
     * Wenn nicht vorhanden, Strukturen im Redis aufbauen. Dazu wird die Datei geladen
     * und pro Zeile verarbeitet.
     * <p/>
     * Die gesamte Zeile wird unter einem DokumentenID im HASH KeyDocuments abgelegt.
     * <p/>
     * Danach wird die Zeile normalisierte Tokens gesplittet. Diese werden dann auf die
     * Menge der enthaltenen (Sub-)Terme expandiert.
     * <p/>
     * Jeder so erzeugte Term wird als eigenes ZSET in Redis gespeichert, dabei ist der
     * Score Wert derzeit nicht verwendet. Es wird also nur die Eigenschaft der ZSETs
     * ausgenutzt, intern alphabetisch zu sortieren.
     */
    public void fillData(Jedis jedis, String inputFile) {
        if (!jedis.exists(KEY_DOCUMENTS)) {
            try {
                LOG.info(String.format("Lade Datei %s in die Datenbank...", inputFile));
                File file = new File(inputFile);
                BufferedReader stream = new BufferedReader(new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8")));
                String line = null;
                while ((line = stream.readLine()) != null) {
                    Long id = nextId(jedis);
                    jedis.hset(KEY_DOCUMENTS, Long.toString(id), line);
                    LOG.info(String.format("%d - %s", id, line));
                    StringTokenizer tokenizer = new StringTokenizer(line.replaceAll("[.-]", " ").toLowerCase(Locale.GERMAN));
                    while (tokenizer.hasMoreTokens()) {
                        String token = tokenizer.nextToken().trim();
                        for (int i = 0; i < token.length(); i++) {
                            for (int j = i + 1; j < token.length(); j++) {
                                String term = token.substring(i, j);
                                jedis.zadd(String.format("%s.%s", KEY_TERMS_BASE, term), 0, Long.toString(id));
                            }
                        }
                    }
                }
            } catch (IOException e) {
                LOG.error("Error reading file:", e);
            }
        } else {
            LOG.info(String.format("Key %s already exists in Redis-DB", KEY_DOCUMENTS));
        }
    }


    private Long nextId(Jedis jedis) {
        return jedis.incr(KEY_DOCUMENT_ID);
    }

    public Jedis startSession() {
        return pool.getResource();
    }

    public void stopSession(Jedis jedis) {
        pool.returnResource(jedis);
    }

    public void destroy() {
        if (pool != null) {
            pool.destroy();
        }
    }

    /**
     * Datenstrukturen im Redis löschen
     */
    public void clean(Jedis jedis) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cleanup Redis-DB");
        }
        // geht eigentlich schneller, führt aber zu einer SocketTimeoutException
        // jedis.flushDB();
        jedis.del(KEY_DOCUMENTS);
        jedis.del(KEY_DOCUMENT_ID);
        Set<String> keys = jedis.keys(String.format("%s.*", KEY_TERMS_BASE));
        for (String key : keys) {
            jedis.del(key);
        }

    }

    /**
     * Lädt die Dokumenteninhalte aus der Redis-DB für die gegebenen DokumentenID's
     *
     * @param jedis Connection
     * @param ids   eine id oder eine Liste von ids
     * @return Liste der gefundenen Dokumente oder <b>null</b> wenn keine gefunden wurden
     */
    public List<String> load(Jedis jedis, String... ids) {
        List<String> result = null;
        if (ids.length > 0) {
            result = jedis.hmget(KEY_DOCUMENTS, ids);
        }
        return result;
    }

    /**
     * Lädt die Dokumente zu einem oder mehreren Suchtermen aus der Redis-DB
     *
     * @param jedis Connection
     * @param terms Ein oder mehrere Suchterme
     * @return Liste der gefundenen Dokumente
     */
    public List<String> complete(Jedis jedis, String... terms) {
        String searchTerm = "tmp";
        if (terms.length > 1) {
            String[] searchTerms = new String[terms.length];
            for (int i = 0; i < terms.length; i++) {
                searchTerms[i] = String.format("%s.%s", KEY_TERMS_BASE, terms[i].toLowerCase());
            }
            jedis.zinterstore(searchTerm, searchTerms);
        } else {
            searchTerm = String.format("%s.%s", KEY_TERMS_BASE, terms[0].toLowerCase());
        }
        Set<String> ids = jedis.zrange(searchTerm, 0, -1);
        return load(jedis, ids.toArray(new String[ids.size()]));
    }


}
