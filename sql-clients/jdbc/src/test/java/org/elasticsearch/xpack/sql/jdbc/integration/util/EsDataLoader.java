/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.util;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.fail;

// used rarely just to load the data (hence why it's marked as abstract)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class EsDataLoader {

    private static final Logger log = ESLoggerFactory.getLogger(EsDataLoader.class.getName());
    private static Client client;

    //@ClassRule
    //public static LocalEs resource = new LocalEs();

    private static void initClient() {
        if (client == null) {
            client = new PreBuiltTransportClient(Settings.EMPTY)
                       .addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(), 9300));
        }
    }
    private static Client client()  {
        return client;
    }

    private static IndicesAdminClient indices() {
        return client().admin().indices();
    }

    //
    // based on: https://github.com/datacharmer/test_db
    //

    @BeforeClass
    public static void loadData() throws Exception {
        initClient();

        loadEmployees();
        //        loadTitles();
        //        loadSalaries();
        //        loadDepartments();
        //        loadDepartmentsToEmployees();
        //
        //        loadDepartmentsToEmployeesNested();
        //        loadEmployeesNested();
        waitForIndices("emp");
    }
    
    public static void afterClass() throws Exception {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    static void loadEmployees() throws Exception {
        String index = "emp";
        String type = "emp";
                
        // create index
        indices().create(new CreateIndexRequest(index)).get();

        // add mapping
        indices().preparePutMapping(index)
        .setType(type)
        .setSource(jsonBuilder()
            .startObject()
            .startObject(type)
            .startObject("properties")
            .startObject("emp_no").field("type", "integer").endObject()
            .startObject("birth_date").field("type", "date").endObject()
//            .startObject("first_name").field("type", "text").endObject()
//            .startObject("last_name").field("type", "text").endObject()
//            .startObject("gender").field("type", "keyword").endObject()
            .startObject("hire_date").field("type", "date").endObject()
            .endObject()
            .endObject()
            .endObject()
            )
        .get();

        loadFromFile("/employees.csv", index, type, "emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date");
    }
    
    private static void loadTitles() throws Exception {
        String index = "title";
        String type = "title";
                
        // create index
        indices().create(new CreateIndexRequest(index)).get();

        // add mapping
        indices().preparePutMapping(index)
        .setType(type)
        .setSource(jsonBuilder()
            .startObject()
            .startObject(type)
            .startObject("properties")
            .startObject("emp_no").field("type", "short").endObject()
//            .startObject("title").field("type", "text").endObject()
            .startObject("from_date").field("type", "date").endObject()
            .startObject("to_date").field("type", "date").endObject()
            .endObject()
            .endObject()
            .endObject()
            )
        .get();

        // add data
        loadFromFile("/titles.csv", index, type, "emp_no", "title", "from_date", "to_date");
    }

    private static void loadSalaries() throws Exception {
        String index = "sal";
        String type = "sal";
                
        // create index
        indices().create(new CreateIndexRequest(index)).get();

        // add mapping
        indices().preparePutMapping(index)
        .setType(type)
        .setSource(jsonBuilder()
            .startObject()
            .startObject(type)
            .startObject("properties")
            .startObject("emp_no").field("type", "short").endObject()
            .startObject("salary").field("type", "integer").endObject()
            .startObject("from_date").field("type", "date").endObject()
            .startObject("to_date").field("type", "date").endObject()
            .endObject()
            .endObject()
            .endObject()
            )
        .get();

        // add data
        loadFromFile("/salaries.csv", index, type, "emp_no", "salary", "from_date", "to_date");
    }

    private static void loadDepartments() throws Exception {
        String index = "dep";
        String type = "dep";
                
        // create index
        indices().create(new CreateIndexRequest(index)).get();

        // add mapping
        indices().preparePutMapping(index)
        .setType(type)
        .setSource(jsonBuilder()
            .startObject()
            .startObject(type)
            .startObject("properties")
              //.startObject("dept_no").field("type", "text").endObject()
              //.startObject("dept_name").field("type", "text").endObject()
            .endObject()
            .endObject()
            .endObject()
            )
        .get();

        // add data
        loadFromFile("/departments.csv", index, type, "dept_no", "dept_name");
    }

    private static void loadDepartmentsToEmployees() throws Exception {
        String index = "deptoemp";
        String type = "deptoemp";
                
        // create index
        indices().create(new CreateIndexRequest(index)).get();
        
        // add mapping
        indices().preparePutMapping(index)
        .setType(type)
        .setSource(jsonBuilder()
            .startObject()
            .startObject(type)
            .startObject("properties")
            .startObject("emp_no").field("type", "short").endObject()
            //.startObject("dept_no").field("type", "text").endObject()
            .startObject("from_date").field("type", "date").endObject()
            .startObject("to_date").field("type", "date").endObject()
            .endObject()
            .endObject()
            .endObject()
            )
        .get();

        // add data
        loadFromFile("/dep_emp.csv", index, type, "emp_no", "dept_no", "from_date", "to_date");
    }
    

    private static void loadDepartmentsToEmployeesNested() throws Exception {
        String index = "nested_deptoemp";
        String type = "nested_deptoemp";
                
        // create index
        indices().create(new CreateIndexRequest(index)).get();
        
        // add mapping
        indices().preparePutMapping(index)
        .setType(type)
        .setSource(jsonBuilder()
            .startObject()
            .startObject(type)
            .startObject("properties")
//            .startObject("dept_no").field("type", "text").endObject()
//            .startObject("dept_name").field("type", "text").endObject()
            .startObject("employee").field("type", "nested")
                .startObject("properties")
                .startObject("emp_no").field("type", "short").endObject()
                .startObject("birth_date").field("type", "date").endObject()
//                .startObject("first_name").field("type", "text").endObject()
//                .startObject("last_name").field("type", "text").endObject()
                .startObject("gender").field("type", "keyword").endObject()
                .startObject("from_date").field("type", "date").endObject()
                .startObject("to_date").field("type", "date").endObject()
                .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            )
        .get();

        log.info("About to parse and load the employee to department nested datasets");

        // read the 3 files and do nested-loop joins in memory before sending the data out
        List<String> deps = Files.readAllLines(Paths.get(EsDataLoader.class.getResource("/departments.csv").toURI()));
        List<String> dep_emp = Files.readAllLines(Paths.get(EsDataLoader.class.getResource("/dep_emp.csv").toURI()));
        List<String> emp = Files.readAllLines(Paths.get(EsDataLoader.class.getResource("/employees.csv").toURI()));


        String[] dCols = { "dept_no", "dept_name" };
        String[] empCol = { "emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date", "from_date", "to_date" };

        BulkRequestBuilder brb = client().prepareBulk();

        deps.forEach(d -> {
            try {
                String[] dSplit = d.split(",");

                // top-level = dep
                XContentBuilder sourceBuilder = jsonBuilder().startObject();
                for (int i = 0; i < dSplit.length; i++) {
                    sourceBuilder.field(dCols[i], dSplit[i]);
                }
                
                sourceBuilder.startArray("employee");
                
                String match = "," + dSplit[0];
                List<String[]> empKeys = dep_emp.stream()
                    .filter(l -> l.contains(match))
                    .map(m -> m.replace(match, ""))
                    .map(m -> m.split(","))
                    .collect(toList());

                for (String[] empKey : empKeys) {
                    String m = empKey[0] + ",";
                    for (String e : emp) {
                        if (e.startsWith(m)) {
                            String[] empSplit = e.split(",");
                            sourceBuilder.startObject();
                            for (int i = 0; i < empSplit.length; i++) {
                                sourceBuilder.field(empCol[i], empSplit[i]);
                            }

                            sourceBuilder.field("from_date", empKey[1]);
                            sourceBuilder.field("to_date", empKey[2]);
                            sourceBuilder.endObject();
                            // found the match, move to the next item in the higher loop
                            break;
                        }
                    }
                }
                sourceBuilder.endArray();
                sourceBuilder.endObject();

                brb.add(client().prepareIndex(index, type).setSource(sourceBuilder));
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        });

        BulkResponse br = brb.get();

        if (br.hasFailures()) {
            fail(br.buildFailureMessage());
        }

        log.info("Dataset loaded in {}", br.getTook().format());
    }

    private static void loadEmployeesNested() throws Exception {
        String index = "demo";
        String type = "employees";
                
        // create index
        indices().create(new CreateIndexRequest(index)).get();
        
        // add mapping
        indices().preparePutMapping(index)
        .setType(type)
        .setSource(jsonBuilder()
            .startObject()
            .startObject(type)
            .startObject("properties")
                .startObject("emp_no").field("type", "integer").endObject()
                .startObject("birth_date").field("type", "date").endObject()
                .startObject("hire_date").field("type", "date").endObject()
                .startObject("department").field("type", "nested")
                    .startObject("properties")
                    .startObject("from_date").field("type", "date").endObject()
                    .startObject("to_date").field("type", "date").endObject()
                .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            )
        .get();        

        log.info("About to parse and load the department to employee nested datasets");

        // read the 3 files and do nested-loop joins in memory before sending the data out
        List<String> deps = Files.readAllLines(Paths.get(EsDataLoader.class.getResource("/departments.csv").toURI()));
        List<String> dep_emp = Files.readAllLines(Paths.get(EsDataLoader.class.getResource("/dep_emp.csv").toURI()));
        List<String> employees = Files.readAllLines(Paths.get(EsDataLoader.class.getResource("/employees.csv").toURI()));


        String[] dCols = { "dept_no", "dept_name" };
        String[] empCol = { "emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date", "from_date", "to_date" };

        BulkRequestBuilder brb = client().prepareBulk();

        employees.forEach(emp -> {
            try {
                String[] eSplit = emp.split(",");

                // top-level = emp
                XContentBuilder sourceBuilder = jsonBuilder().startObject();
                for (int i = 0; i < eSplit.length; i++) {
                    sourceBuilder.field(empCol[i], eSplit[i]);
                }
                
                sourceBuilder.startArray("department");
                
                String match = eSplit[0] + ",";
                List<String[]> depKeys = dep_emp.stream()
                    .filter(l -> l.contains(match))
                    .map(m -> m.replace(match, ""))
                    .map(m -> m.split(","))
                    .collect(toList());

                for (String[] depKey : depKeys) {
                    String m = depKey[0] + ",";
                    for (String dep : deps) {
                        if (dep.startsWith(m)) {
                            String[] depSplit = dep.split(",");
                            sourceBuilder.startObject();
                            for (int i = 0; i < depSplit.length; i++) {
                                sourceBuilder.field(dCols[i], depSplit[i]);
                            }

                            sourceBuilder.field("from_date", depKey[1]);
                            sourceBuilder.field("to_date", depKey[2]);
                            sourceBuilder.endObject();
                            // found the match, move to the next item in the higher loop
                            break;
                        }
                    }
                }
                sourceBuilder.endArray();
                sourceBuilder.endObject();

                brb.add(client().prepareIndex(index, type).setSource(sourceBuilder));
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        });

        BulkResponse br = brb.get();

        if (br.hasFailures()) {
            fail(br.buildFailureMessage());
        }

        log.info("Dataset loaded in {}", br.getTook().format());
    }
    
    private static void loadEmployeesNotAnalyzedNested() throws Exception {
        String index = "emp";
        String type = "emp";
                
        // create index
        indices().create(new CreateIndexRequest(index)).get();
        
        // add mapping
        indices().preparePutMapping(index)
        .setType(type)
        .setSource(jsonBuilder()
            .startObject()
            .startObject(type)
            .startObject("properties")
                .startObject("emp_no").field("type", "integer").endObject()
                .startObject("birth_date").field("type", "date").endObject()
//                .startObject("first_name").field("type", "keyword").endObject()
//                .startObject("last_name").field("type", "keyword").endObject()
//                .startObject("gender").field("type", "keyword").endObject()
                .startObject("tenure").field("type", "integer").endObject()
                .startObject("salary").field("type", "integer").endObject()
                .startObject("dep").field("type", "nested")
                    .startObject("properties")
                    .startObject("from_date").field("type", "date").endObject()
//                    .startObject("dept_name").field("type", "keyword").endObject()
                    .startObject("to_date").field("type", "date").endObject()
                .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            )
        .get();


        log.info("About to parse and load the department to employee nested datasets");

        // read the 3 files and do nested-loop joins in memory before sending the data out
        List<String> deps = Files.readAllLines(Paths.get(EsDataLoader.class.getResource("/departments.csv").toURI()));
        List<String> dep_emp = Files.readAllLines(Paths.get(EsDataLoader.class.getResource("/dep_emp.csv").toURI()));
        List<String> employees = Files.readAllLines(Paths.get(EsDataLoader.class.getResource("/employees.csv").toURI()));


        String[] dCols = { "dept_no", "dept_name" };
        String[] empCol = { "emp_no", "birth_date", "first_name", "last_name", "gender", "tenure", "from_date", "to_date" };

        BulkRequestBuilder brb = client().prepareBulk();

        Random rnd = new Random();
        employees.forEach(emp -> {
            try {
                String[] eSplit = emp.split(",");

                // compute age from birth_date
                Integer year = Integer.parseInt(eSplit[5].substring(0, 4));
                eSplit[5] = String.valueOf(2017 - year);

                // top-level = emp
                XContentBuilder sourceBuilder = jsonBuilder().startObject();
                for (int i = 0; i < eSplit.length; i++) {
                    sourceBuilder.field(empCol[i], eSplit[i]);
                }
                // salary (random between 38000 and 106000)
                sourceBuilder.field("salary", rnd.nextInt(106000 - 38000 + 1) + 38000);
                
                sourceBuilder.startArray("dep");
                
                String match = eSplit[0] + ",";
                List<String[]> depKeys = dep_emp.stream()
                    .filter(l -> l.contains(match))
                    .map(m -> m.replace(match, ""))
                    .map(m -> m.split(","))
                    .collect(toList());

                for (String[] depKey : depKeys) {
                    String m = depKey[0] + ",";
                    for (String dep : deps) {
                        if (dep.startsWith(m)) {
                            String[] depSplit = dep.split(",");
                            sourceBuilder.startObject();
                            for (int i = 0; i < depSplit.length; i++) {
                                sourceBuilder.field(dCols[i], depSplit[i]);
                            }

                            sourceBuilder.field("from_date", depKey[1]);
                            sourceBuilder.field("to_date", depKey[2]);
                            sourceBuilder.endObject();
                            // found the match, move to the next item in the higher loop
                            break;
                        }
                    }
                }
                sourceBuilder.endArray();
                sourceBuilder.endObject();

                brb.add(client().prepareIndex(index, type).setSource(sourceBuilder));
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        });

        BulkResponse br = brb.get();

        if (br.hasFailures()) {
            fail(br.buildFailureMessage());
        }

        log.info("Dataset loaded in {}", br.getTook().format());
    }
    
    private static void waitForIndices(String...indices) {
        for (String index : indices) {
            // force refresh
            indices().prepareRefresh(index).get(TimeValue.timeValueSeconds(5));
            // wait for index to fully start
            client().admin().cluster().prepareHealth(index).setTimeout(TimeValue.timeValueSeconds(10)).setWaitForYellowStatus().get();
        }
    }

    private static void loadFromFile(String resourceName, String index, String type, String... columns) throws Exception {
        URL dataSet = EsDataLoader.class.getResource(resourceName);

        log.info("About to parse and load dataset {}", dataSet);

        BulkRequestBuilder brb = client().prepareBulk();

        try (Stream<String> stream = Files.lines(Paths.get(dataSet.toURI()))) {
            stream.forEach(s -> {
                try {
                    XContentBuilder sourceBuilder = jsonBuilder().startObject();
                    String[] lineSplit = s.split(",");
                    for (int i = 0; i < lineSplit.length; i++) {
                        sourceBuilder.field(columns[i], lineSplit[i]);
                    }
                    sourceBuilder.endObject();
                    brb.add(client().prepareIndex(index, type).setSource(sourceBuilder));
                } catch (IOException ex) {
                    throw new IllegalStateException(ex);
                }
            });
        }

        BulkResponse br = brb.get();

        if (br.hasFailures()) {
            fail(br.buildFailureMessage());
        }

        log.info("Dataset loaded in {}", br.getTook().format());
    }

    @Test
    public void testNoOp() {}

}