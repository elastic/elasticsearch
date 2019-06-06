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
package org.elasticsearch.client.documentation;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.client.graph.Connection;
import org.elasticsearch.client.graph.GraphExploreRequest;
import org.elasticsearch.client.graph.GraphExploreResponse;
import org.elasticsearch.client.graph.Hop;
import org.elasticsearch.client.graph.Vertex;
import org.elasticsearch.client.graph.VertexRequest;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;

public class GraphDocumentationIT extends ESRestHighLevelClientTestCase {
    
    
    @Before
    public void indexDocuments() throws IOException {
        // Create chain of doc IDs across indices 1->2->3
        Request doc1 = new Request(HttpPut.METHOD_NAME, "/index1/_doc/1");
        doc1.setJsonEntity("{ \"participants\":[1,2], \"text\":\"let's start projectx\", \"attachment_md5\":\"324FHDGHFDG4564\"}");
        client().performRequest(doc1);
        
        Request doc2 = new Request(HttpPut.METHOD_NAME, "/index2/_doc/2");
        doc2.setJsonEntity("{\"participants\":[2,3,4], \"text\":\"got something you both may be interested in\"}");
        client().performRequest(doc2);        

        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));            
    }     

    @SuppressForbidden(reason = "system out is ok for a documentation example")
    public void testExplore() throws Exception {
        RestHighLevelClient client = highLevelClient();
        
        
        
        // tag::x-pack-graph-explore-request
        GraphExploreRequest request = new GraphExploreRequest();
        request.indices("index1", "index2");
        request.useSignificance(false);
        TermQueryBuilder startingQuery = new TermQueryBuilder("text", "projectx"); 
        
        Hop hop1 = request.createNextHop(startingQuery); // <1>
        VertexRequest people = hop1.addVertexRequest("participants"); // <2>
        people.minDocCount(1);
        VertexRequest files = hop1.addVertexRequest("attachment_md5");
        files.minDocCount(1);

        Hop hop2 = request.createNextHop(null); // <3>
        VertexRequest vr2 = hop2.addVertexRequest("participants");
        vr2.minDocCount(5);

        GraphExploreResponse exploreResponse = client.graph().explore(request, RequestOptions.DEFAULT); // <4>
        // end::x-pack-graph-explore-request

        
        // tag::x-pack-graph-explore-response
        Collection<Vertex> v = exploreResponse.getVertices();
        Collection<Connection> c = exploreResponse.getConnections();
        for (Vertex vertex : v) {
            System.out.println(vertex.getField() + ":" + vertex.getTerm() + // <1>
                    " discovered at hop depth " + vertex.getHopDepth());
        }
        for (Connection link : c) {
            System.out.println(link.getFrom() + " -> " + link.getTo() // <2>
                    + " evidenced by " + link.getDocCount() + " docs");
        }
        // end::x-pack-graph-explore-response
        
        
        Collection<Vertex> initialVertices = exploreResponse.getVertices();
        
        // tag::x-pack-graph-explore-expand
        GraphExploreRequest expandRequest = new GraphExploreRequest();
        expandRequest.indices("index1", "index2");

        
        Hop expandHop1 = expandRequest.createNextHop(null); // <1>
        VertexRequest fromPeople = expandHop1.addVertexRequest("participants"); // <2>
        for (Vertex vertex : initialVertices) {
            if (vertex.getField().equals("participants")) {
                fromPeople.addInclude(vertex.getTerm(), 1f);
            }
        }
        
        Hop expandHop2 = expandRequest.createNextHop(null); 
        VertexRequest newPeople = expandHop2.addVertexRequest("participants"); // <3>
        for (Vertex vertex : initialVertices) {
            if (vertex.getField().equals("participants")) {
                newPeople.addExclude(vertex.getTerm());
            }
        }          

        GraphExploreResponse expandResponse = client.graph().explore(expandRequest, RequestOptions.DEFAULT);      
        // end::x-pack-graph-explore-expand        
        
    }

}
