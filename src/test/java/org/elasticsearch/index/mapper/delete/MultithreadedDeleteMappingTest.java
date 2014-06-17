/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper.delete;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class MultithreadedDeleteMappingTest extends ElasticsearchIntegrationTest {

    class CreateThread extends Thread{
        public void run(){
            for (int i=0; i<1000; ++i){
                try {
                    CreateIndexResponse cir = client().admin().indices().prepareCreate("test_index").setSource(" {\"mappings\":{" +
                            "                \"test_type\":{" +
                            "                 \"properties\":{" +
                            "                    \"text\":{" +
                            "                      \"type\":     \"string\"," +
                            "                      \"analyzer\": \"whitespace\"}" +
                            "                    }" +
                            "                  }" +
                            "                 }" +
                            "}").execute().actionGet();
                    assertTrue(cir.isAcknowledged());
                } catch( IndexAlreadyExistsException e ){

                }
            }
        }
    }

    class DeleteThread extends Thread{
        public void run() {
            for (int i=0; i<1000; ++i) {
                try {
                    DeleteMappingResponse dmr = client().admin().indices().prepareDeleteMapping("test_index").setType("test_type").execute().actionGet();
                    assertTrue(dmr.isAcknowledged());
                    client().admin().indices().prepareDelete("test_index").execute().actionGet();
                } catch ( IndexMissingException e) {

                } catch( TypeMissingException te ){

                }
            }
        }
    }

    public void startAll(List<Thread> threads){
        for(Thread t : threads){
            t.start();
        }
    }

    public void joinAll(List<Thread> threads) {
        for (Thread t : threads) {
            boolean joined = false;
            while(!joined) {
                try {
                    t.join();
                    joined = true;
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    @Test
    public void testMultiThreadedMappingTest(){
        List<Thread> createList = new ArrayList<>();
        List<Thread> deleteList = new ArrayList<>();
        for (int i = 0; i<10; ++i) {
            createList.add(new CreateThread());
        }

        for (int i =0; i<10; ++i){
            deleteList.add(new DeleteThread());
        }

        startAll(createList);
        startAll(deleteList);

        joinAll(createList);
        joinAll(deleteList);
    }
}
