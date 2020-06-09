/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SysColumnsTestCase extends JdbcIntegrationTestCase {


    public void testMergeSameMapping() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("emp_no").field("type", "integer").endObject();
            builder.startObject("first_name").field("type", "text").endObject();
            builder.startObject("gender").field("type", "keyword").endObject();
            builder.startObject("languages").field("type", "byte").endObject();
            builder.startObject("last_name").field("type", "text").endObject();
            builder.startObject("salary").field("type", "integer").endObject();
            builder.startObject("_meta_field").field("type", "keyword").endObject();
        });
        
        createIndexWithMapping("test2", builder -> {
            builder.startObject("emp_no").field("type", "integer").endObject();
            builder.startObject("first_name").field("type", "text").endObject();
            builder.startObject("gender").field("type", "keyword").endObject();
            builder.startObject("languages").field("type", "byte").endObject();
            builder.startObject("last_name").field("type", "text").endObject();
            builder.startObject("salary").field("type", "integer").endObject();
            builder.startObject("_meta_field").field("type", "keyword").endObject();
        });
        
        assertResultsForSysColumnsForTableQuery("test%", new String[][] {
            {"test%"      ,"_meta_field","KEYWORD"},
            {"test%"      ,"emp_no"     ,"INTEGER"},
            {"test%"      ,"first_name" ,"TEXT"},
            {"test%"      ,"gender"     ,"KEYWORD"},
            {"test%"      ,"languages"  ,"BYTE"},
            {"test%"      ,"last_name"  ,"TEXT"},
            {"test%"      ,"salary"     ,"INTEGER"}
        });
    }
    public void testMultiLevelObjectMappings() throws Exception {
        createIndexWithMapping("test", builder -> {
            builder.startObject("test")
                .startObject("properties")
                    .startObject("test")
                        .field("type", "text")
                        .startObject("fields")
                            .startObject("keyword")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("bar")
                        .field("type", "text")
                        .startObject("fields")
                            .startObject("keyword")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
            builder.startObject("bar")
                .field("type", "text")
                .startObject("fields")
                    .startObject("keyword")
                        .field("type", "keyword")
                    .endObject()
                .endObject()
            .endObject();
        });
        
        assertResultsForSysColumnsQuery(new String[][] {
            {"test"      ,"bar"              ,"TEXT"},
            {"test"      ,"bar.keyword"      ,"KEYWORD"},
            {"test"      ,"test.bar"         ,"TEXT"},
            {"test"      ,"test.bar.keyword" ,"KEYWORD"},
            {"test"      ,"test.test"        ,"TEXT"},
            {"test"      ,"test.test.keyword","KEYWORD"}
        });
        assertResultsForShowColumnsForTableQuery("test", new String[][] {
            {"bar"              ,"VARCHAR"        ,"text"},
            {"bar.keyword"      ,"VARCHAR"        ,"keyword"},
            {"test"             ,"STRUCT"         ,"object"},
            {"test.bar"         ,"VARCHAR"        ,"text"},
            {"test.bar.keyword" ,"VARCHAR"        ,"keyword"},
            {"test.test"        ,"VARCHAR"        ,"text"},
            {"test.test.keyword","VARCHAR"        ,"keyword"}
        });
    }

    public void testMultiLevelNestedMappings() throws Exception {
        createIndexWithMapping("test", builder -> {
            builder.startObject("dep")
                .field("type", "nested")
                .startObject("properties")
                    .startObject("dep_name")
                        .field("type", "text")
                    .endObject()
                    .startObject("dep_no")
                        .field("type", "text")
                        .startObject("fields")
                            .startObject("keyword")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("end_date")
                        .field("type", "date")
                    .endObject()
                    .startObject("start_date")
                        .field("type", "date")
                    .endObject()
                .endObject()
            .endObject();
        });
        
        assertResultsForSysColumnsQuery(new String[][] {});
        assertResultsForShowColumnsForTableQuery("test", new String[][] {
            {"dep"               ,"STRUCT"         ,"nested"},
            {"dep.dep_name"      ,"VARCHAR"        ,"text"},
            {"dep.dep_no"        ,"VARCHAR"        ,"text"},
            {"dep.dep_no.keyword","VARCHAR"        ,"keyword"},
            {"dep.end_date"      ,"TIMESTAMP"      ,"datetime"},
            {"dep.start_date"    ,"TIMESTAMP"      ,"datetime"},
        });
    }

    public void testAliasWithIncompatibleTypes() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("value").field("type", "double").endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "text").endObject();
            builder.startObject("value").field("type", "double").endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
        });

        assertResultsForSysColumnsQuery(new String[][] {
            {"test1"      ,"id"      ,"KEYWORD"},
            {"test1"      ,"value"   ,"DOUBLE"},
            {"test2"      ,"id"      ,"TEXT"},
            {"test2"      ,"value"   ,"DOUBLE"},
            {"test_alias" ,"value"   ,"DOUBLE"}
        });
    }

    public void testAliasWithIncompatibleSearchableProperty() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("value").field("type", "boolean").endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "keyword").field("index", false).endObject();
            builder.startObject("value").field("type", "boolean").endObject();
        });

        createIndexWithMapping("test3", builder -> {
            builder.startObject("id").field("type", "keyword").field("index", false).endObject();
            builder.startObject("value").field("type", "boolean").endObject();
        });

        createIndexWithMapping("test4", builder -> {
            builder.startObject("id").field("type", "keyword").field("index", false).endObject();
            builder.startObject("value").field("type", "boolean").endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test3").field("alias", "test_alias2").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test4").field("alias", "test_alias2").endObject().endObject();
        });

        assertResultsForSysColumnsQuery(new String[][] {
            {"test1"       ,"id"      ,"KEYWORD"},
            {"test1"       ,"value"   ,"BOOLEAN"},
            {"test2"       ,"id"      ,"KEYWORD"},
            {"test2"       ,"value"   ,"BOOLEAN"},
            {"test3"       ,"id"      ,"KEYWORD"},
            {"test3"       ,"value"   ,"BOOLEAN"},
            {"test4"       ,"id"      ,"KEYWORD"},
            {"test4"       ,"value"   ,"BOOLEAN"},
            {"test_alias"  ,"value"   ,"BOOLEAN"},
            {"test_alias2" ,"id"      ,"KEYWORD"},
            {"test_alias2" ,"value"   ,"BOOLEAN"}
        });
    }

    public void testAliasWithIncompatibleAggregatableProperty() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "text").field("fielddata", true).endObject();
            builder.startObject("value").field("type", "date").endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "text").endObject();
            builder.startObject("value").field("type", "date").endObject();
        });

        createIndexWithMapping("test3", builder -> {
            builder.startObject("id").field("type", "text").field("fielddata", true).endObject();
            builder.startObject("value").field("type", "date").endObject();
        });

        createIndexWithMapping("test4", builder -> {
            builder.startObject("id").field("type", "text").field("fielddata", true).endObject();
            builder.startObject("value").field("type", "date").endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test3").field("alias", "test_alias2").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test4").field("alias", "test_alias2").endObject().endObject();
        });

        assertResultsForSysColumnsQuery(new String[][] {
            {"test1"      ,"id"      ,"TEXT"},
            {"test1"      ,"value"   ,"DATETIME"},
            {"test2"      ,"id"      ,"TEXT"},
            {"test2"      ,"value"   ,"DATETIME"},
            {"test3"      ,"id"      ,"TEXT"},
            {"test3"      ,"value"   ,"DATETIME"},
            {"test4"      ,"id"      ,"TEXT"},
            {"test4"      ,"value"   ,"DATETIME"},
            {"test_alias" ,"value"   ,"DATETIME"},
            {"test_alias2","id"      ,"TEXT"},
            {"test_alias2","value"   ,"DATETIME"},
        });
    }

    public void testAliasWithIncompatibleTypesInSubfield() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "date")
                .startObject("fields")
                .startObject("raw")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "date")
                .startObject("fields")
                .startObject("raw")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
        });

        assertResultsForSysColumnsQuery(new String[][] {
            {"test1"      ,"id"       ,"TEXT"},
            {"test1"      ,"id.raw"   ,"KEYWORD"},
            {"test1"      ,"value"    ,"DATETIME"},
            {"test1"      ,"value.raw","LONG"},
            {"test2"      ,"id"       ,"TEXT"},
            {"test2"      ,"id.raw"   ,"INTEGER"},
            {"test2"      ,"value"    ,"DATETIME"},
            {"test2"      ,"value.raw","LONG"},
            {"test_alias" ,"id"       ,"TEXT"},
            {"test_alias" ,"value"    ,"DATETIME"},
            {"test_alias" ,"value.raw","LONG"},
        });
    }

    public void testAliasWithIncompatibleSearchablePropertyInSubfield() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "date")
                .startObject("fields")
                .startObject("raw")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "integer")
                .field("index", false)
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "date")
                .startObject("fields")
                .startObject("raw")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
        });

        assertResultsForSysColumnsQuery(new String[][] {
            {"test1"      ,"id"       ,"TEXT"},
            {"test1"      ,"id.raw"   ,"INTEGER"},
            {"test1"      ,"value"    ,"DATETIME"},
            {"test1"      ,"value.raw","LONG"},
            {"test2"      ,"id"       ,"TEXT"},
            {"test2"      ,"id.raw"   ,"INTEGER"},
            {"test2"      ,"value"    ,"DATETIME"},
            {"test2"      ,"value.raw","LONG"},
            {"test_alias" ,"id"       ,"TEXT"},
            {"test_alias" ,"value"    ,"DATETIME"},
            {"test_alias" ,"value.raw","LONG"},
        });
    }

    public void testAliasWithIncompatibleAggregatablePropertyInSubfield() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "ip")
                .startObject("fields")
                .startObject("raw")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "ip")
                .startObject("fields")
                .startObject("raw")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
        });

        assertResultsForSysColumnsQuery(new String[][] {
            {"test1"      ,"id"       ,"TEXT"},
            {"test1"      ,"id.raw"   ,"INTEGER"},
            {"test1"      ,"value"    ,"IP"},
            {"test1"      ,"value.raw","TEXT"},
            {"test2"      ,"id"       ,"TEXT"},
            {"test2"      ,"id.raw"   ,"INTEGER"},
            {"test2"      ,"value"    ,"IP"},
            {"test2"      ,"value.raw","TEXT"},
            {"test_alias" ,"id"       ,"TEXT"},
            {"test_alias" ,"value"    ,"IP"},
            {"test_alias" ,"value.raw","TEXT"},
        });
    }

    public void testAliasWithSubfieldsAndDifferentRootFields() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name")
                .field("type", "keyword")
                .field("index", false)
                .startObject("fields")
                .startObject("raw")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
        });

        assertResultsForSysColumnsQuery(new String[][] {
            {"test1"      ,"id"      ,"KEYWORD"},
            {"test1"      ,"name"    ,"TEXT"},
            {"test1"      ,"name.raw","KEYWORD"},
            {"test2"      ,"id"      ,"KEYWORD"},
            {"test2"      ,"name"    ,"KEYWORD"},
            {"test2"      ,"name.raw","KEYWORD"},
            {"test_alias" ,"id"      ,"KEYWORD"}
        });
    }

    public void testAliasWithSubfieldsAndDifferentRootFields_AndObjects() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name")
                .field("type", "text")
                .startObject("fields")
                    .startObject("raw")
                        .field("type", "keyword")
                    .endObject()
                .endObject()
            .endObject();
            builder.startObject("address")
                .startObject("properties")
                    .startObject("city")
                        .field("type", "text")
                        .startObject("fields")
                            .startObject("raw")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("county")
                        .field("type", "keyword")
                        .startObject("fields")
                            .startObject("raw")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
               .endObject()
            .endObject();
        });
        
        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name")
                .field("type", "keyword")                               // <-------- first difference in mapping
                .startObject("fields")
                    .startObject("raw")
                        .field("type", "keyword")
                    .endObject()
                .endObject()
            .endObject();
            builder.startObject("address")
                .startObject("properties")
                    .startObject("city")
                        .field("type", "text")
                        .startObject("fields")
                            .startObject("raw")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("county")
                        .field("type", "text")                          // <-------- second difference in mapping
                        .startObject("fields")
                            .startObject("raw")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
               .endObject()
            .endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject(); 
        });
        
        assertResultsForSysColumnsQuery(new String[][] {
            {"test1"      ,"address.city"      ,"TEXT"},
            {"test1"      ,"address.city.raw"  ,"KEYWORD"},
            {"test1"      ,"address.county"    ,"KEYWORD"},
            {"test1"      ,"address.county.raw","KEYWORD"},
            {"test1"      ,"id"                ,"KEYWORD"},
            {"test1"      ,"name"              ,"TEXT"},
            {"test1"      ,"name.raw"          ,"KEYWORD"},
            {"test2"      ,"address.city"      ,"TEXT"},
            {"test2"      ,"address.city.raw"  ,"KEYWORD"},
            {"test2"      ,"address.county"    ,"TEXT"},
            {"test2"      ,"address.county.raw","KEYWORD"},
            {"test2"      ,"id"                ,"KEYWORD"},
            {"test2"      ,"name"              ,"KEYWORD"},
            {"test2"      ,"name.raw"          ,"KEYWORD"},
            {"test_alias" ,"address.city"      ,"TEXT"},
            {"test_alias" ,"address.city.raw"  ,"KEYWORD"},
            {"test_alias" ,"id"                ,"KEYWORD"}
            // address.county gets removed since it has conflicting mappings
        });
    }

    public void testAliasWithSubfieldsAndDifferentRootFields_AndObjects_2() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name")
                .field("type", "text")
                .startObject("fields")
                    .startObject("raw")
                        .field("type", "keyword")
                    .endObject()
                .endObject()
            .endObject();
            builder.startObject("address")
                .startObject("properties")
                    .startObject("home")
                        .startObject("properties")
                            .startObject("city")
                                .field("type", "text")
                                .startObject("fields")
                                    .startObject("raw")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("county")
                                .field("type", "keyword")
                                .startObject("fields")
                                    .startObject("raw")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("work")
                        .startObject("properties")
                            .startObject("name")
                                .field("type", "keyword")
                                .startObject("fields")
                                    .startObject("raw")
                                        .field("type", "text")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("location")
                                .field("type", "text")
                            .endObject()
                        .endObject()
                   .endObject()
               .endObject()
            .endObject();
        });
        
        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name")
                .field("type", "keyword")                               // <-------- first difference in mapping
                .startObject("fields")
                    .startObject("raw")
                        .field("type", "keyword")
                    .endObject()
                .endObject()
            .endObject();
            builder.startObject("address")
                .startObject("properties")
                    .startObject("home")
                        .startObject("properties")
                            .startObject("city")
                                .field("type", "text")
                                .startObject("fields")
                                    .startObject("raw")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("county")
                                .field("type", "text")                  // <-------- second difference in mapping
                                .startObject("fields")
                                    .startObject("raw")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("work")
                        .startObject("properties")
                            .startObject("name")
                                .field("type", "keyword")
                                .startObject("fields")
                                    .startObject("raw")
                                        .field("type", "keyword")       // <-------- third difference in mapping
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("location")
                                .field("type", "text")
                            .endObject()
                        .endObject()
                   .endObject()
               .endObject()
            .endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject(); 
        });
        
        assertResultsForSysColumnsQuery(new String[][] {
            {"test1"          ,"address.home.city"      ,"TEXT"},
            {"test1"          ,"address.home.city.raw"  ,"KEYWORD"},
            {"test1"          ,"address.home.county"    ,"KEYWORD"},// field type is different and its children will not make it in alias
            {"test1"          ,"address.home.county.raw","KEYWORD"},
            {"test1"          ,"address.work.location"  ,"TEXT"},
            {"test1"          ,"address.work.name"      ,"KEYWORD"},
            {"test1"          ,"address.work.name.raw"  ,"TEXT"},   // field type is different and it will not make it in alias
            {"test1"          ,"id"                     ,"KEYWORD"},
            {"test1"          ,"name"                   ,"TEXT"},
            {"test1"          ,"name.raw"               ,"KEYWORD"},
            {"test2"          ,"address.home.city"      ,"TEXT"},
            {"test2"          ,"address.home.city.raw"  ,"KEYWORD"},
            {"test2"          ,"address.home.county"    ,"TEXT"},   // field type is different and its children will not make it in alias
            {"test2"          ,"address.home.county.raw","KEYWORD"},
            {"test2"          ,"address.work.location"  ,"TEXT"},
            {"test2"          ,"address.work.name"      ,"KEYWORD"},
            {"test2"          ,"address.work.name.raw"  ,"KEYWORD"},// field type is different and it will not make it in alias
            {"test2"          ,"id"                     ,"KEYWORD"},
            {"test2"          ,"name"                   ,"KEYWORD"},
            {"test2"          ,"name.raw"               ,"KEYWORD"},
            {"test_alias"     ,"address.home.city"      ,"TEXT"},
            {"test_alias"     ,"address.home.city.raw"  ,"KEYWORD"},
            {"test_alias"     ,"address.work.location"  ,"TEXT"},
            {"test_alias"     ,"address.work.name"      ,"KEYWORD"},
            {"test_alias"     ,"id"                     ,"KEYWORD"}
            // address.home.county gets removed since it has conflicting mappings
        });
    }
    
    public void testMultiIndicesMultiAlias() throws Exception {
        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name").field("type", "text").endObject();
        });
        createIndexWithMapping("test4", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name").field("type", "text").field("index", false).endObject();
        });
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name").field("type", "keyword").endObject();
            builder.startObject("number").field("type", "long").endObject();
        });
        createIndexWithMapping("test3", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name").field("type", "keyword").endObject();
            builder.startObject("number").field("type", "long").endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "alias1").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test1").field("alias", "alias2").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "alias2").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "alias3").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test4").field("alias", "alias3").endObject().endObject();
        });

        assertResultsForSysColumnsQuery(new String[][] {
            {"alias1","id"    ,"KEYWORD"},
            {"alias1","name"  ,"KEYWORD"},
            {"alias1","number","LONG"},
            {"alias2","id"    ,"KEYWORD"},
            {"alias2","number","LONG"},
            {"alias3","id"    ,"KEYWORD"},
            {"test1" ,"id"    ,"KEYWORD"},
            {"test1" ,"name"  ,"KEYWORD"},
            {"test1" ,"number","LONG"},
            {"test2" ,"id"    ,"KEYWORD"},
            {"test2" ,"name"  ,"TEXT"},
            {"test3" ,"id"    ,"KEYWORD"},
            {"test3" ,"name"  ,"KEYWORD"},
            {"test3" ,"number","LONG"},
            {"test4" ,"id"    ,"KEYWORD"},
            {"test4" ,"name"  ,"TEXT"}
        });
    }

    private static void createIndexWithMapping(String indexName, CheckedConsumer<XContentBuilder, IOException> mapping) throws Exception {
        createIndex(indexName);
        updateMapping(indexName, mapping);
    }

    private void doWithQuery(String query, CheckedConsumer<ResultSet, SQLException> consumer) throws SQLException {
        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                try (ResultSet results = statement.executeQuery()) {
                    consumer.accept(results);
                }
            }
        }
    }

    private static void createAliases(CheckedConsumer<XContentBuilder, IOException> definitions) throws Exception {
        Request request = new Request("POST", "/_aliases");
        XContentBuilder createAliases = JsonXContent.contentBuilder().startObject();
        createAliases.startArray("actions");
        {
            definitions.accept(createAliases);
        }
        createAliases.endArray();
        createAliases.endObject();
        request.setJsonEntity(Strings.toString(createAliases));
        client().performRequest(request);
    }

    private void assertResultsForSysColumnsQuery(String[][] rows) throws Exception {
        assertResultsForSysColumnsForTableQuery(null, rows);
    }

    private void assertResultsForSysColumnsForTableQuery(String index, String[][] rows) throws Exception {
        String query = "SYS COLUMNS" + (index == null ? "" : (" TABLE LIKE '" + index + "'"));
        doWithQuery(query, (results) -> {
            for (String[] row : rows) {
                results.next();
                assertEquals(row[0], results.getString(3)); // table name
                assertEquals(row[1], results.getString(4)); // column name
                assertEquals(row[2], results.getString(6)); // type name
            }
            assertFalse(results.next());
        });
    }

    private void assertResultsForShowColumnsForTableQuery(String index, String[][] rows) throws Exception {
        doWithQuery("SHOW COLUMNS FROM " + index, (results) -> {
            for (String[] row : rows) {
                results.next();
                assertEquals(row[0], results.getString("column"));
                assertEquals(row[1], results.getString("type"));
                assertEquals(row[2], results.getString("mapping"));
            }
            assertFalse(results.next());
        });
    }
}
