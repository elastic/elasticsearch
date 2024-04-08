/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery.ec2;

import com.amazonaws.util.DateUtils;

import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URLEncodedUtils;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.fixture.AbstractHttpFixture;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * {@link AmazonEC2Fixture} is a fixture that emulates an AWS EC2 service.
 */
public class AmazonEC2Fixture extends AbstractHttpFixture {

    private static final String IMDSV_2_TOKEN = "imdsv2-token";
    private static final String X_AWS_EC_2_METADATA_TOKEN = "X-aws-ec2-metadata-token";

    private final Path nodes;
    private final boolean instanceProfile;
    private final boolean containerCredentials;

    private AmazonEC2Fixture(final String workingDir, final String nodesUriPath, boolean instanceProfile, boolean containerCredentials) {
        super(workingDir);
        this.nodes = toPath(Objects.requireNonNull(nodesUriPath));
        this.instanceProfile = instanceProfile;
        this.containerCredentials = containerCredentials;
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("AmazonEC2Fixture <working directory> <nodes transport uri file>");
        }

        boolean instanceProfile = Booleans.parseBoolean(System.getenv("ACTIVATE_INSTANCE_PROFILE"), false);
        boolean containerCredentials = Booleans.parseBoolean(System.getenv("ACTIVATE_CONTAINER_CREDENTIALS"), false);

        final AmazonEC2Fixture fixture = new AmazonEC2Fixture(args[0], args[1], instanceProfile, containerCredentials);
        fixture.listen();
    }

    @Override
    protected Response handle(final Request request) throws IOException {
        if ("/".equals(request.getPath()) && (HttpPost.METHOD_NAME.equals(request.getMethod()))) {
            final String userAgent = request.getHeader("User-Agent");
            if (userAgent != null && userAgent.startsWith("aws-sdk-java")) {

                final String auth = request.getHeader("Authorization");
                if (auth == null || auth.contains("ec2_integration_test_access_key") == false) {
                    throw new IllegalArgumentException("wrong access key: " + auth);
                }

                // Simulate an EC2 DescribeInstancesResponse
                byte[] responseBody = EMPTY_BYTE;
                for (NameValuePair parse : URLEncodedUtils.parse(new String(request.getBody(), UTF_8), UTF_8)) {
                    if ("Action".equals(parse.getName())) {
                        responseBody = generateDescribeInstancesResponse();
                        break;
                    }
                }
                return new Response(RestStatus.OK.getStatus(), contentType("text/xml; charset=UTF-8"), responseBody);
            }
        }
        if ("/latest/meta-data/local-ipv4".equals(request.getPath())
            && (HttpGet.METHOD_NAME.equals(request.getMethod()))
            && request.getHeaders().getOrDefault(X_AWS_EC_2_METADATA_TOKEN, "").equals(IMDSV_2_TOKEN)) {
            return new Response(RestStatus.OK.getStatus(), TEXT_PLAIN_CONTENT_TYPE, "127.0.0.1".getBytes(UTF_8));
        }

        if (instanceProfile
            && "/latest/meta-data/iam/security-credentials/".equals(request.getPath())
            && HttpGet.METHOD_NAME.equals(request.getMethod())
            && request.getHeaders().getOrDefault(X_AWS_EC_2_METADATA_TOKEN, "").equals(IMDSV_2_TOKEN)) {
            final Map<String, String> headers = new HashMap<>(contentType("text/plain"));
            return new Response(RestStatus.OK.getStatus(), headers, "my_iam_profile".getBytes(UTF_8));
        }

        if ("/latest/api/token".equals(request.getPath()) && HttpPut.METHOD_NAME.equals(request.getMethod())) {
            return new Response(RestStatus.OK.getStatus(), TEXT_PLAIN_CONTENT_TYPE, IMDSV_2_TOKEN.getBytes(StandardCharsets.UTF_8));
        }

        if ((containerCredentials
            && "/ecs_credentials_endpoint".equals(request.getPath())
            && HttpGet.METHOD_NAME.equals(request.getMethod()))
            || ("/latest/meta-data/iam/security-credentials/my_iam_profile".equals(request.getPath())
                && HttpGet.METHOD_NAME.equals(request.getMethod())
                && request.getHeaders().getOrDefault(X_AWS_EC_2_METADATA_TOKEN, "").equals(IMDSV_2_TOKEN))) {
            final Date expiration = new Date(new Date().getTime() + TimeUnit.DAYS.toMillis(1));
            final String response = String.format(Locale.ROOT, """
                {
                  "AccessKeyId": "ec2_integration_test_access_key",
                  "Expiration": "%s",
                  "RoleArn": "test",
                  "SecretAccessKey": "ec2_integration_test_secret_key",
                  "Token": "test"
                }""", DateUtils.formatISO8601Date(expiration));

            final Map<String, String> headers = new HashMap<>(contentType("application/json"));
            return new Response(RestStatus.OK.getStatus(), headers, response.getBytes(UTF_8));
        }

        return null;
    }

    /**
     * Generates a XML response that describe the EC2 instances
     */
    private byte[] generateDescribeInstancesResponse() {
        final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
        xmlOutputFactory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);

        final StringWriter out = new StringWriter();
        XMLStreamWriter sw;
        try {
            sw = xmlOutputFactory.createXMLStreamWriter(out);
            sw.writeStartDocument();

            String namespace = "http://ec2.amazonaws.com/doc/2013-02-01/";
            sw.setDefaultNamespace(namespace);
            sw.writeStartElement(XMLConstants.DEFAULT_NS_PREFIX, "DescribeInstancesResponse", namespace);
            {
                sw.writeStartElement("requestId");
                sw.writeCharacters(UUID.randomUUID().toString());
                sw.writeEndElement();

                sw.writeStartElement("reservationSet");
                {
                    if (Files.exists(nodes)) {
                        for (String address : Files.readAllLines(nodes)) {

                            sw.writeStartElement("item");
                            {
                                sw.writeStartElement("reservationId");
                                sw.writeCharacters(UUID.randomUUID().toString());
                                sw.writeEndElement();

                                sw.writeStartElement("instancesSet");
                                {
                                    sw.writeStartElement("item");
                                    {
                                        sw.writeStartElement("instanceId");
                                        sw.writeCharacters(UUID.randomUUID().toString());
                                        sw.writeEndElement();

                                        sw.writeStartElement("imageId");
                                        sw.writeCharacters(UUID.randomUUID().toString());
                                        sw.writeEndElement();

                                        sw.writeStartElement("instanceState");
                                        {
                                            sw.writeStartElement("code");
                                            sw.writeCharacters("16");
                                            sw.writeEndElement();

                                            sw.writeStartElement("name");
                                            sw.writeCharacters("running");
                                            sw.writeEndElement();
                                        }
                                        sw.writeEndElement();

                                        sw.writeStartElement("privateDnsName");
                                        sw.writeCharacters(address);
                                        sw.writeEndElement();

                                        sw.writeStartElement("dnsName");
                                        sw.writeCharacters(address);
                                        sw.writeEndElement();

                                        sw.writeStartElement("instanceType");
                                        sw.writeCharacters("m1.medium");
                                        sw.writeEndElement();

                                        sw.writeStartElement("placement");
                                        {
                                            sw.writeStartElement("availabilityZone");
                                            sw.writeCharacters("use-east-1e");
                                            sw.writeEndElement();

                                            sw.writeEmptyElement("groupName");

                                            sw.writeStartElement("tenancy");
                                            sw.writeCharacters("default");
                                            sw.writeEndElement();
                                        }
                                        sw.writeEndElement();

                                        sw.writeStartElement("privateIpAddress");
                                        sw.writeCharacters(address);
                                        sw.writeEndElement();

                                        sw.writeStartElement("ipAddress");
                                        sw.writeCharacters(address);
                                        sw.writeEndElement();
                                    }
                                    sw.writeEndElement();
                                }
                                sw.writeEndElement();
                            }
                            sw.writeEndElement();
                        }
                    }
                    sw.writeEndElement();
                }
                sw.writeEndElement();

                sw.writeEndDocument();
                sw.flush();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return out.toString().getBytes(UTF_8);
    }

    @SuppressForbidden(reason = "Paths#get is fine - we don't have environment here")
    private static Path toPath(final String dir) {
        return Paths.get(dir);
    }
}
