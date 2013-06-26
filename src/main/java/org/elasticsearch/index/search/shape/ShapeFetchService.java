/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.search.shape;

import com.spatial4j.core.shape.Shape;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.geo.GeoJSONShapeParser;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Service which retrieves pre-indexed Shapes from another index
 */
public class ShapeFetchService extends AbstractComponent {

    private final Client client;

    @Inject
    public ShapeFetchService(Client client, Settings settings) {
        super(settings);
        this.client = client;
    }

    /**
     * Fetches the Shape with the given ID in the given type and index.
     *
     * @param id         ID of the Shape to fetch
     * @param type       Index type where the Shape is indexed
     * @param index      Index where the Shape is indexed
     * @param shapeField Name of the field in the Shape Document where the Shape itself is located
     * @return Shape with the given ID
     * @throws IOException Can be thrown while parsing the Shape Document and extracting the Shape
     */
    public Shape fetch(String id, String type, String index, String shapeField) throws IOException {
        GetResponse response = client.get(new GetRequest(index, type, id).preference("_local").operationThreaded(false)).actionGet();
        if (!response.isExists()) {
            throw new ElasticSearchIllegalArgumentException("Shape with ID [" + id + "] in type [" + type + "] not found");
        }

        XContentParser parser = null;
        try {
            parser = XContentHelper.createParser(response.getSourceAsBytesRef());
            XContentParser.Token currentToken;
            while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (currentToken == XContentParser.Token.FIELD_NAME) {
                    if (shapeField.equals(parser.currentName())) {
                        parser.nextToken();
                        return GeoJSONShapeParser.parse(parser);
                    } else {
                        parser.nextToken();
                        parser.skipChildren();
                    }
                }
            }
            throw new ElasticSearchIllegalStateException("Shape with name [" + id + "] found but missing " + shapeField + " field");
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}
