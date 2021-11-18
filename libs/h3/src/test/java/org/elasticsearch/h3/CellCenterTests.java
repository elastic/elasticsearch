/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.h3;

import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

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
        InputStream fis = getClass().getResourceAsStream(file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
        String line = reader.readLine();
        while (line != null) {
            StringTokenizer tokenizer = new StringTokenizer(line, " ");
            assertEquals(3, tokenizer.countTokens());
            String h3Address = tokenizer.nextToken();
            assertEquals(h3Address, true, H3.h3IsValid(h3Address));
            double lat = Double.parseDouble(tokenizer.nextToken());
            double lon = Double.parseDouble(tokenizer.nextToken());
            LatLng latLng = H3.h3ToLatLng(h3Address);
            assertEquals(h3Address, lat, latLng.getLatDeg(), 1e-6);
            assertEquals(h3Address, lon, latLng.getLonDeg(), 1e-6);
            String computedH3Address = H3.geoToH3Address(lat, lon, H3Index.H3_get_resolution(H3.stringToH3(h3Address)));
            assertEquals(h3Address, computedH3Address);
            assertEquals(h3Address, computedH3Address);
            line = reader.readLine();
        }
    }
}
