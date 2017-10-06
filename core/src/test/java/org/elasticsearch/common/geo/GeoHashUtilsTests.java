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

package org.elasticsearch.common.geo;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class GeoHashUtilsTests extends ESTestCase {

    public void testNeighbors() {
        // Simple root case
        assertThat(GeoHashUtils.addNeighbors("7", new ArrayList<String>()),
                containsInAnyOrder("4", "5", "6", "d", "e", "h", "k", "s"));

        // Root cases (Outer cells)
        assertThat(GeoHashUtils.addNeighbors("0", new ArrayList<String>()),
                containsInAnyOrder("1", "2", "3", "p", "r"));
        assertThat(GeoHashUtils.addNeighbors("b", new ArrayList<String>()),
                containsInAnyOrder("8", "9", "c", "x", "z"));
        assertThat(GeoHashUtils.addNeighbors("p", new ArrayList<String>()),
                containsInAnyOrder("n", "q", "r", "0", "2"));
        assertThat(GeoHashUtils.addNeighbors("z", new ArrayList<String>()),
                containsInAnyOrder("8", "b", "w", "x", "y"));

        // Root crossing dateline
        assertThat(GeoHashUtils.addNeighbors("2", new ArrayList<String>()),
                containsInAnyOrder("0", "1", "3", "8", "9", "p", "r", "x"));
        assertThat(GeoHashUtils.addNeighbors("r", new ArrayList<String>()),
                containsInAnyOrder("0", "2", "8", "n", "p", "q", "w", "x"));

        // level1: simple case
        assertThat(GeoHashUtils.addNeighbors("dk", new ArrayList<String>()),
                containsInAnyOrder("d5", "d7", "de", "dh", "dj", "dm", "ds", "dt"));

        // Level1: crossing cells
        assertThat(GeoHashUtils.addNeighbors("d5", new ArrayList<String>()),
                containsInAnyOrder("d4", "d6", "d7", "dh", "dk", "9f", "9g", "9u"));
        assertThat(GeoHashUtils.addNeighbors("d0", new ArrayList<String>()),
                containsInAnyOrder("d1", "d2", "d3", "9b", "9c", "6p", "6r", "3z"));
    }

}
