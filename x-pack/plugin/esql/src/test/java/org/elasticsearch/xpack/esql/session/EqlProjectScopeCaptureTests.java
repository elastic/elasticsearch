/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.search.crossproject.ProjectRoutingInfo;
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;

public class EqlProjectScopeCaptureTests extends ESTestCase {
    public void testRoutingCannotChangeBetweenFromAndEqlSchemaResolution() {
        assumeTrue("Requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(alias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        var preAnalysis = new PreAnalyzer().preAnalyze(TEST_PARSER.parseQuery("EQL logs \"any where true\""));
        var capture = EsqlSession.getRoutingInfoCapture(executionInfo, preAnalysis);
        TargetProjects first = scope("a");
        capture.accept(first);
        expectThrows(EqlIllegalArgumentException.class, () -> capture.accept(scope("b")));
        assertSame(first, executionInfo.getResolvedTargetProjects());
        EsqlSession.getRoutingInfoCapture(executionInfo, preAnalysis).accept(first);
        expectThrows(
            EqlIllegalArgumentException.class,
            () -> EsqlSession.getRoutingInfoCapture(executionInfo, preAnalysis).accept(scope("b"))
        );
    }

    private static TargetProjects scope(String alias) {
        return new TargetProjects(null, List.of(new ProjectRoutingInfo(ProjectId.fromId(alias), "elasticsearch", alias, "org", null)));
    }
}
