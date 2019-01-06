/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.nio.BufferUnderflowException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class LicenseTests extends ESTestCase {

    public void testFromXContent() throws Exception {

        String licenseString = "{\"license\":" +
            "{\"uid\":\"4056779d-b823-4c12-a9cb-efa4a8d8c422\"," +
            "\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1546589020459," +
            "\"expiry_date_in_millis\":1546596340459," +
            "\"max_nodes\":5," +
            "\"issued_to\":\"customer\"," +
            "\"issuer\":\"elasticsearch\"," +
            "\"signature\":\"AAAAAgAAAA34V2kfTJVtvdL2LttwAAABmFJ6NGRnbEM3WVQrZVQwNkdKQmR1VytlMTMyM1J0dTZ1WGwyY2ZCVFhqMGtJU2gzZ3pnNTVpOW" +
            "F5Y1NaUkwyN2VsTEtCYnlZR2c5WWtjQ0phaDlhRjlDUXViUmUwMWhjSkE2TFcwSGdneTJHbUV4N2RHUWJxV20ybjRsZHRzV2xkN0ZmdDlYblJmNVcxMlBWeU81" +
            "V1hLUm1EK0V1dmF3cFdlSGZzTU5SZE1qUmFra3JkS1hCanBWVmVTaFFwV3BVZERzeG9Sci9rYnlJK2toODZXY09tNmFHUVNUL3IyUHExV3VSTlBneWNJcFQ0bX" +
            "l0cmhNNnRwbE1CWE4zWjJ5eGFuWFo0NGhsb3B5WFd1eTdYbFFWQkxFVFFPSlBERlB0eVVJYXVSZ0lsR2JpRS9rN1h4MSsvNUpOcGN6cU1NOHN1cHNtSTFIUGN1" +
            "bWNGNEcxekhrblhNOXZ2VEQvYmRzQUFwbytUZEpRR3l6QU5oS2ZFSFdSbGxxNDZyZ0xvUHIwRjdBL2JqcnJnNGFlK09Cek9pYlJ5Umc9PQAAAQAth77fQLF7CC" +
            "EL7wA6Z0/UuRm/weECcsjW/50kBnPLO8yEs+9/bPa5LSU0bF6byEXOVeO0ebUQfztpjulbXh8TrBDSG+6VdxGtohPo2IYPBaXzGs3LOOor6An/lhptxBWdwYmf" +
            "bcp0m8mnXZh1vN9rmbTsZXnhBIoPTaRDwUBi3vJ3Ms3iLaEm4S8Slrfmtht2jUjgGZ2vAeZ9OHU2YsGtrSpz6f\"}";
        License license = License.fromSource(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON);
        assertThat(license.type(), equalTo("gold"));
        assertThat(license.uid(), equalTo("4056779d-b823-4c12-a9cb-efa4a8d8c422"));
        assertThat(license.issuer(), equalTo("elasticsearch"));
        assertThat(license.issuedTo(), equalTo("customer"));
        assertThat(license.expiryDate(), equalTo(1546596340459L));
        assertThat(license.issueDate(), equalTo(1546589020459L));
    }

    public void testNotEnoughBytesFromXContent() throws Exception {

        String licenseString = "{\"license\": " +
            "{\"uid\":\"4056779d-b823-4c12-a9cb-efa4a8d8c422\"," +
            "\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1546589020459," +
            "\"expiry_date_in_millis\":1546596340459," +
            "\"max_nodes\":5," +
            "\"issued_to\":\"customer\"," +
            "\"issuer\":\"elasticsearch\"," +
            "\"signature\":\"AA\"}" +
            "}";
        ElasticsearchException exception =
            expectThrows(ElasticsearchException.class,
                () -> {
                    License.fromSource(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)),
                        XContentType.JSON);
                });
        assertThat(exception.getMessage(), containsString("malformed signature for license [4056779d-b823-4c12-a9cb-efa4a8d8c422]"));
        assertThat(exception.getCause(), instanceOf(BufferUnderflowException.class));
    }

    public void testMalformedSignatureFromXContent() throws Exception {

        String licenseString = "{\"license\": " +
            "{\"uid\":\"4056779d-b823-4c12-a9cb-efa4a8d8c422\"," +
            "\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1546589020459," +
            "\"expiry_date_in_millis\":1546596340459," +
            "\"max_nodes\":5," +
            "\"issued_to\":\"customer\"," +
            "\"issuer\":\"elasticsearch\"," +
            "\"signature\":\"" + randomAlphaOfLength(10) + "\"}" +
            "}";
        ElasticsearchException exception =
            expectThrows(ElasticsearchException.class,
                () -> {
                    License.fromSource(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)),
                        XContentType.JSON);
                });
    }

    public void testUnableToBase64DecodeFromXContent() throws Exception {

        String licenseString = "{\"license\":" +
            "{\"uid\":\"4056779d-b823-4c12-a9cb-efa4a8d8c422\"," +
            "\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1546589020459," +
            "\"expiry_date_in_millis\":1546596340459," +
            "\"max_nodes\":5," +
            "\"issued_to\":\"customer\"," +
            "\"issuer\":\"elasticsearch\"," +
            "\"signature\":\"AAAAAgAAAA34V2kfTJVtvdL2LttwAAABmFJ6NGRnbEM3WVQrZVQwNkdKQmR1VytlMTMyM1J0dTZ1WGwyY2ZCVFhqMGtJU2gzZ3pnNTVpOW" +
            "F5Y1NaUkwyN2VsTEtCYnlZR2c5WWtjQ0phaDlhRjlDUXViUmUwMWhjSkE2TFcwSGdneTJHbUV4N2RHUWJxV20ybjRsZHRzV2xkN0ZmdDlYblJmNVcxMlBWeU81" +
            "V1hLUm1EK0V1dmF3cFdlSGZzTU5SZE1qUmFra3JkS1hCanBWVmVTaFFwV3BVZERzeG9Sci9rYnlJK2toODZXY09tNmFHUVNUL3IyUHExV3VSTlBneWNJcFQ0bX" +
            "l0cmhNNnRwbE1CWE4zWjJ5eGFuWFo0NGhsb3B5WFd1eTdYbFFWQkxFVFFPSlBERlB0eVVJYXVSZ0lsR2JpRS9rN1h4MSsvNUpOcGN6cU1NOHN1cHNtSTFIUGN1" +
            "bWNGNEcxekhrblhNOXZ2VEQvYmRzQUFwbytUZEpRR3l6QU5oS2ZFSFdSbGxxNDZyZ0xvUHIwRjdBL2JqcnJnNGFlK09Cek9pYlJ5Umc9PQAAAQAth77fQLF7CC" +
            "EL7wA6Z0/UuRm/weECcsjW/50kBnPLO8yEs+9/bPa5LSU0bF6byEXOVeO0ebUQfztpjulbXh8TrBDSG+6VdxGtohPo2IYPBaXzGs3LOOor6An/lhptxBWdwYmf" +
            "+xHAQ8tyvRqP5G+PRU7tiluEwR/eyHGZV2exdJNzmoGzdPSWwueBM5HK2GexORICH+UFI4cuGz444/hL2MMM1RdpVWQkT0SJ6D9x/VuSmHuYPdtX59Pp41LXvl" +
            "bcp0m8mnXZh1vN9rmbTsZXnhBIoPTaRDwUBi3vJ3Ms3iLaEm4S8Slrfmtht2jUjgGZ2vAeZ9OHU2YsGtrSpz6fd\"}";
        ElasticsearchException exception =
            expectThrows(ElasticsearchException.class,
                () -> {
                    License.fromSource(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)),
                        XContentType.JSON);
                });
        assertThat(exception.getMessage(), containsString("malformed signature for license [4056779d-b823-4c12-a9cb-efa4a8d8c422]"));
        assertThat(exception.getCause(), instanceOf(IllegalArgumentException.class));
    }
}
