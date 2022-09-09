/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.h3;

import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

public class CellCenterTests extends ESTestCase {

    public void testRes0() throws Exception {
        processFile("res00ic.txt");
    }

    public void testRes1() throws Exception {
        processFile("res01ic.txt");
    }

    public void testRes2() throws Exception {
        processFile("res02ic.txt");
    }

    public void testRes3() throws Exception {
        processFile("res03ic.txt");
    }

    public void testBc05r08centers() throws Exception {
        processFile("bc05r08centers.txt");
    }

    public void testBc05r09centers() throws Exception {
        processFile("bc05r09centers.txt");
    }

    public void testBc05r10centers() throws Exception {
        processFile("bc05r10centers.txt");
    }

    public void testBc05r11centers() throws Exception {
        processFile("bc05r11centers.txt");
    }

    public void testBc05r12centers() throws Exception {
        processFile("bc05r12centers.txt");
    }

    public void testBc05r13centers() throws Exception {
        processFile("bc05r13centers.txt");
    }

    public void testBc05r05centers() throws Exception {
        processFile("bc05r14centers.txt");
    }

    public void testBc05r15centers() throws Exception {
        processFile("bc05r15centers.txt");
    }

    public void testBc14r08centers() throws Exception {
        processFile("bc14r08centers.txt");
    }

    public void testBc14r09centers() throws Exception {
        processFile("bc14r09centers.txt");
    }

    public void testBc14r10centers() throws Exception {
        processFile("bc14r10centers.txt");
    }

    public void testBc14r11centers() throws Exception {
        processFile("bc14r11centers.txt");
    }

    public void testBc14r12centers() throws Exception {
        processFile("bc14r12centers.txt");
    }

    public void testBc14r13centers() throws Exception {
        processFile("bc14r13centers.txt");
    }

    public void testBc14r14centers() throws Exception {
        processFile("bc14r14centers.txt");
    }

    public void testBc14r15centers() throws Exception {
        processFile("bc14r15centers.txt");
    }

    public void testBc19r08centers() throws Exception {
        processFile("bc19r08centers.txt");
    }

    public void testBc19r09centers() throws Exception {
        processFile("bc19r09centers.txt");
    }

    public void testBc19r10centers() throws Exception {
        processFile("bc19r10centers.txt");
    }

    public void testBc19r11centers() throws Exception {
        processFile("bc19r11centers.txt");
    }

    public void testBc19r12centers() throws Exception {
        processFile("bc19r12centers.txt");
    }

    public void testBc19r13centers() throws Exception {
        processFile("bc19r13centers.txt");
    }

    public void testBc19r14centers() throws Exception {
        processFile("bc19r14centers.txt");
    }

    public void testBc19r15centers() throws Exception {
        processFile("bc19r15centers.txt");
    }

    private void processFile(String file) throws IOException {
        InputStream fis = getClass().getResourceAsStream(file + ".gz");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(fis), StandardCharsets.UTF_8));
        String line = reader.readLine();
        while (line != null) {
            StringTokenizer tokenizer = new StringTokenizer(line, " ");
            assertEquals(3, tokenizer.countTokens());
            String h3Address = tokenizer.nextToken();
            assertEquals(h3Address, true, H3.h3IsValid(h3Address));
            double lat = Double.parseDouble(tokenizer.nextToken());
            double lon = Double.parseDouble(tokenizer.nextToken());
            assertH3ToLatLng(h3Address, lat, lon);
            assertGeoToH3(h3Address, lat, lon);
            assertHexRing(h3Address);
            line = reader.readLine();
        }
    }

    private void assertH3ToLatLng(String h3Address, double lat, double lon) {
        LatLng latLng = H3.h3ToLatLng(h3Address);
        assertEquals(h3Address, lat, latLng.getLatDeg(), 1e-6);
        assertEquals(h3Address, lon, latLng.getLonDeg(), 1e-6);
    }

    private void assertGeoToH3(String h3Address, double lat, double lon) {
        String computedH3Address = H3.geoToH3Address(lat, lon, H3Index.H3_get_resolution(H3.stringToH3(h3Address)));
        assertEquals(h3Address, computedH3Address);
        assertEquals(h3Address, computedH3Address);
    }

    private void assertHexRing(String h3Address) {
        String[] neighbors = H3.hexRing(h3Address);
        long center = H3.stringToH3(h3Address);
        for (String neighbor : neighbors) {
            long l = H3.stringToH3(neighbor);
            assertEquals(H3Index.H3_get_resolution(center), H3Index.H3_get_resolution(l));
        }
    }
}
