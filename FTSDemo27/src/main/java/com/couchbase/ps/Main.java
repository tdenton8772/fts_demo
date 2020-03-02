package com.couchbase.ps;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.queries.MatchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class Main {
    private static Properties m_props;
    private static File propFile;
    static private Cluster cluster;
    static private Bucket bucket;


    public static void main(String[] args) {
        m_props = new Properties();
        propFile = new File("configuration.properties");

        try {
            LoadProperties(propFile);
        } catch (Exception e) {
            System.out.println(e);
        }

        String dbClusterList = m_props.getProperty("cluster");
        List<String> nodes = Arrays.asList(dbClusterList.split("\\s*,\\s*"));

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .mutationTokensEnabled(true)
                .build();

        cluster = CouchbaseCluster.create(env, nodes);
        cluster.authenticate(m_props.getProperty("username"), m_props.getProperty("password"));
        bucket = cluster.openBucket(m_props.getProperty("bucket"));

        JsonDocument doc = JsonDocument.create("test-doc", JsonObject.create()
                .put("name", "test")
                .put("type", "tester"));
        JsonDocument holdingVar = bucket.upsert(doc);
        System.out.println(holdingVar);

        String indexName = "unstored";
        MatchQuery query = SearchQuery.match("test");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query)
                        .limit(10)
                        .consistentWith(holdingVar)
                );

        System.out.println("Simple Text Query: " + result);
    }

    public static void LoadProperties(File f) throws IOException {
        FileInputStream propStream = null;
        propStream = new FileInputStream(f);
        m_props.load(propStream);
        propStream.close();
    }
}
