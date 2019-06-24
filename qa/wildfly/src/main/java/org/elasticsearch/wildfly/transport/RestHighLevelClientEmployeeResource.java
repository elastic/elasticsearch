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

package org.elasticsearch.wildfly.transport;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.wildfly.model.Employee;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Path("/employees")
public class RestHighLevelClientEmployeeResource {

    @Inject
    private RestHighLevelClient client;

    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getEmployeeById(final @PathParam("id") Long id) throws IOException {
        Objects.requireNonNull(id);
        final GetResponse response = client.get(new GetRequest("megacorp", Long.toString(id)), RequestOptions.DEFAULT);
        if (response.isExists()) {
            final Map<String, Object> source = response.getSource();
            final Employee employee = new Employee();
            employee.setFirstName((String) source.get("first_name"));
            employee.setLastName((String) source.get("last_name"));
            employee.setAge((Integer) source.get("age"));
            employee.setAbout((String) source.get("about"));
            @SuppressWarnings("unchecked") final List<String> interests = (List<String>) source.get("interests");
            employee.setInterests(interests);
            return Response.ok(employee).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    @PUT
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response putEmployeeById(final @PathParam("id") Long id, final Employee employee) throws URISyntaxException, IOException {
        Objects.requireNonNull(id);
        Objects.requireNonNull(employee);
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.field("first_name", employee.getFirstName());
                builder.field("last_name", employee.getLastName());
                builder.field("age", employee.getAge());
                builder.field("about", employee.getAbout());
                if (employee.getInterests() != null) {
                    builder.startArray("interests");
                    {
                        for (final String interest : employee.getInterests()) {
                            builder.value(interest);
                        }
                    }
                    builder.endArray();
                }
            }
            builder.endObject();
            final IndexRequest request = new IndexRequest("megacorp");
            request.id(Long.toString(id));
            request.source(builder);
            final IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            if (response.status().getStatus() == 201) {
                return Response.created(new URI("/employees/" + id)).build();
            } else {
                return Response.ok().build();
            }
        }
    }

}
