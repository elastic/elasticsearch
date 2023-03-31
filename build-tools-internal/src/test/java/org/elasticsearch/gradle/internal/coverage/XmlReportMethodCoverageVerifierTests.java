/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.coverage;

import org.apache.commons.lang.text.StrSubstitutor;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;

public class XmlReportMethodCoverageVerifierTests {

    @Test
    public void test100percentageCoverage() throws IOException {
        String xml = templateReport(
            Map.of(
                "element1InitMissed",
                0,
                "element1InitCovered",
                10,
                "element2InitMissed",
                0,
                "element2InitCovered",
                10,
                "element2writeToMissed",
                0,
                "element2writeToCovered",
                10
            )
        );

        XmlReportMethodCoverageVerifier verifier = new XmlReportMethodCoverageVerifier(Collections.emptySet());
        List<XmlReportMethodCoverageVerifier.Failure> failures = verifier.verifyReport(xml.getBytes());
        assertThat(failures, Matchers.empty());
    }

    @Test
    public void testSingleLowCoverage() throws IOException {
        String xml = templateReport(
            Map.of(
                "element1InitMissed",
                10,
                "element1InitCovered",
                10,
                "element2InitMissed",
                0,
                "element2InitCovered",
                10,
                "element2writeToMissed",
                0,
                "element2writeToCovered",
                10
            )
        );
        XmlReportMethodCoverageVerifier verifier = new XmlReportMethodCoverageVerifier(Collections.emptySet());
        List<XmlReportMethodCoverageVerifier.Failure> failures = verifier.verifyReport(xml.getBytes());
        assertThat(
            failures,
            Matchers.contains(
                new XmlReportMethodCoverageVerifier.Failure(
                    "org/elasticsearch/aggregations/bucket/adjacency/InternalAdjacencyMatrix",
                    "<init>",
                    0.5
                )
            )
        );

    }

    @Test
    public void testMultipleLowCoverage() throws IOException {
        String xml = templateReport(
            Map.of(
                "element1InitMissed",
                10,
                "element1InitCovered",
                10,
                "element2InitMissed",
                20,
                "element2InitCovered",
                0,
                "element2writeToMissed",
                15,
                "element2writeToCovered",
                10
            )
        );
        XmlReportMethodCoverageVerifier verifier = new XmlReportMethodCoverageVerifier(Collections.emptySet());
        List<XmlReportMethodCoverageVerifier.Failure> failures = verifier.verifyReport(xml.getBytes());
        assertThat(
            failures,
            Matchers.contains(
                new XmlReportMethodCoverageVerifier.Failure(
                    "org/elasticsearch/aggregations/bucket/adjacency/InternalAdjacencyMatrix",
                    "<init>",
                    0.5
                ),
                new XmlReportMethodCoverageVerifier.Failure(
                    "org/elasticsearch/aggregations/bucket/adjacency/AdjacencyMatrixAggregator$KeyedFilter",
                    "<init>",
                    0.0
                ),
                new XmlReportMethodCoverageVerifier.Failure(
                    "org/elasticsearch/aggregations/bucket/adjacency/AdjacencyMatrixAggregator$KeyedFilter",
                    "writeTo",
                    0.4
                )
            )
        );
    }

    @Test
    public void testSkipClasses() throws IOException {
        String xml = templateReport(
            Map.of(
                "element1InitMissed",
                10,
                "element1InitCovered",
                0,
                "element2InitMissed",
                10,
                "element2InitCovered",
                0,
                "element2writeToMissed",
                0,
                "element2writeToCovered",
                10
            )
        );
        Set<String> classesToSkip = Set.of("org/elasticsearch/aggregations/bucket/adjacency/AdjacencyMatrixAggregator$KeyedFilter");
        XmlReportMethodCoverageVerifier verifier = new XmlReportMethodCoverageVerifier(classesToSkip);
        List<XmlReportMethodCoverageVerifier.Failure> failures = verifier.verifyReport(xml.getBytes());
        assertThat(
            failures,
            Matchers.contains(
                new XmlReportMethodCoverageVerifier.Failure(
                    "org/elasticsearch/aggregations/bucket/adjacency/InternalAdjacencyMatrix",
                    "<init>",
                    0.0
                )
            )
        );
    }

    private String templateReport(Map<String, Integer> params) {
        return StrSubstitutor.replace(xml, params);
    }

    String xml = """
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?><!DOCTYPE report PUBLIC "-//JACOCO//DTD Report 1.1//EN"
                "report.dtd">
        <report name="aggregations">
            <package name="org/elasticsearch/aggregations/bucket/adjacency">
                <class name="org/elasticsearch/aggregations/bucket/adjacency/InternalAdjacencyMatrix"
                       sourcefilename="InternalAdjacencyMatrix.java">
                    <method name="&lt;init&gt;" desc="(Ljava/lang/String;Ljava/util/List;Ljava/util/Map;)V" line="124">
                        <counter type="INSTRUCTION" missed="0" covered="8"/>
                        <counter type="LINE" missed="0" covered="3"/>
                        <counter type="COMPLEXITY" missed="0" covered="1"/>
                        <counter type="METHOD" missed="0" covered="1"/>
                    </method>
                    <method name="&lt;init&gt;" desc="(Lorg/elasticsearch/common/io/stream/StreamInput;)V" line="132">
                        <counter type="INSTRUCTION" missed="${element1InitMissed}" covered="${element1InitCovered}"/>
                        <counter type="BRANCH" missed="0" covered="2"/>
                        <counter type="LINE" missed="0" covered="8"/>
                        <counter type="COMPLEXITY" missed="0" covered="2"/>
                        <counter type="METHOD" missed="0" covered="1"/>
                    </method>
                </class>
               <class name="org/elasticsearch/aggregations/bucket/adjacency/AdjacencyMatrixAggregator$KeyedFilter"
                                       sourcefilename="AdjacencyMatrixAggregator.java">
                    <method name="&lt;init&gt;" desc="(Lorg/elasticsearch/common/io/stream/StreamInput;)V" line="73">
                        <counter type="INSTRUCTION" missed="${element2InitMissed}" covered="${element2InitCovered}"/>
                        <counter type="LINE" missed="0" covered="4"/>
                        <counter type="COMPLEXITY" missed="0" covered="1"/>
                        <counter type="METHOD" missed="0" covered="1"/>
                    </method>
                    <method name="writeTo" desc="(Lorg/elasticsearch/common/io/stream/StreamOutput;)V" line="80">
                        <counter type="INSTRUCTION" missed="${element2writeToMissed}" covered="${element2writeToCovered}"/>
                        <counter type="LINE" missed="0" covered="3"/>
                        <counter type="COMPLEXITY" missed="0" covered="1"/>
                        <counter type="METHOD" missed="0" covered="1"/>
                    </method>
               </class>
            </package>
            <package name="org/elasticsearch/aggregations">
                    <class name="org/elasticsearch/aggregations/AggregationsPainlessExtension"
                           sourcefilename="AggregationsPainlessExtension.java">
                        <method name="&lt;init&gt;" desc="()V" line="25">
                            <counter type="INSTRUCTION" missed="3" covered="0"/>
                            <counter type="LINE" missed="1" covered="0"/>
                            <counter type="COMPLEXITY" missed="1" covered="0"/>
                            <counter type="METHOD" missed="1" covered="0"/>
                        </method>
                    </class>
                </package>
        </report>
        """;
}
