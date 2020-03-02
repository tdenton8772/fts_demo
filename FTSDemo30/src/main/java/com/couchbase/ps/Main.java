package com.couchbase.ps;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.*;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchRow;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;


public class Main {
    private static Properties m_props;
    private static File propFile;

    public static void main(String[] args) throws InterruptedException{
        m_props = new Properties();
        propFile = new File("configuration.properties");

        try {
            LoadProperties(propFile);
        } catch (Exception e) {
            System.out.println(e);
        }

        ClusterEnvironment env = ClusterEnvironment
                .builder()
                .ioConfig(IoConfig
                    .enableMutationTokens(true)
                    .captureTraffic(ServiceType.KV, ServiceType.QUERY, ServiceType.SEARCH))
                .timeoutConfig(TimeoutConfig
//                    .connectTimeout(Duration.ofMillis(2500))
                    .kvTimeout(Duration.ofMillis(250))
                    .searchTimeout(Duration.ofMillis(500))
                )
                .build();

        Cluster cluster = Cluster.connect(m_props.getProperty("cluster"), ClusterOptions
                    .clusterOptions(m_props.getProperty("username"), m_props.getProperty("password"))
                    .environment(env));
        Bucket main = cluster.bucket(m_props.getProperty("bucket"));
        Collection collection = main.defaultCollection();

        Thread.sleep(5000);
        JsonObject content = JsonObject.create()
                .put("name", "test")
                .put("type", "tester");

        System.out.println(content);
        MutationResult result = collection.upsert("test-doc", content);
        MutationState mutationState = MutationState.from(result.mutationToken().get());

        System.out.println("Upsert successful: " +  result);

        try {
            final SearchResult search_result = cluster
                    .searchQuery("unstored",
                            SearchQuery.match("test"),
                            SearchOptions.searchOptions()
                                    .consistentWith(mutationState));

            for (SearchRow row : search_result.rows()) {
                System.out.println("Found row: " + row);
            }
        } catch (CouchbaseException ex) {
            ex.printStackTrace();
        }

    }

    public static void LoadProperties(File f) throws IOException {
        FileInputStream propStream = null;
        propStream = new FileInputStream(f);
        m_props.load(propStream);
        propStream.close();
    }
}
