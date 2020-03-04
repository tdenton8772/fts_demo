package com.couchbase.ps;

import com.couchbase.client.core.tracing.ThresholdLogReporter;
import com.couchbase.client.core.tracing.ThresholdLogTracer;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.queries.MatchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import io.opentracing.Tracer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


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

        Tracer tracer = ThresholdLogTracer.create(ThresholdLogReporter.builder()
                .kvThreshold(1, TimeUnit.MICROSECONDS)
                .n1qlThreshold(1, TimeUnit.MICROSECONDS)
                .ftsThreshold(1, TimeUnit.MICROSECONDS)
                .analyticsThreshold(1, TimeUnit.MICROSECONDS)
                .logInterval(10, TimeUnit.SECONDS)
                .sampleSize(Integer.MAX_VALUE)
                .pretty(true)
                .build());

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .tracer(tracer)
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

        String indexName = m_props.getProperty("fts_index");
        MatchQuery query = SearchQuery.match("test");

        SearchQueryResult result = bucket.query(
                new SearchQuery(indexName, query)
                        .limit(10)
                        .consistentWith(holdingVar)
                );

        System.out.println("Simple Text Query: " + result);

        JsonDocument test_doc = bucket.get("test-doc");
        if(test_doc != null){
            bucket.upsert(test_doc);
        }

        N1qlQueryResult n1qlHoldingVar = bucket.query(N1qlQuery.simple("Select * from `" + m_props.getProperty("bucket") + "` limit 100;"));

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void LoadProperties(File f) throws IOException {
        FileInputStream propStream = null;
        propStream = new FileInputStream(f);
        m_props.load(propStream);
        propStream.close();
    }
}
