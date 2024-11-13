/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.stats.Metrics;

import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzerDefaultMapping;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultEnrichResolution;
import static org.hamcrest.Matchers.containsString;

public class CheckLicenseTests extends ESTestCase {

    private final EsqlParser parser = new EsqlParser();
    private final String esql = "from tests | eval dummy() | LIMIT 10";

    public void testLicense() {
        for (License.OperationMode functionLicense : License.OperationMode.values()) {
            final LicensedFeature functionLicenseFeature = random().nextBoolean()
                ? LicensedFeature.momentary("test", "dummy", functionLicense)
                : LicensedFeature.persistent("test", "dummy", functionLicense);
            final EsqlFunctionRegistry.FunctionBuilder builder = (source, expression, cfg) -> new DummyAggregateFunction(
                source,
                expression,
                functionLicenseFeature
            );
            for (License.OperationMode operationMode : License.OperationMode.values()) {
                if (License.OperationMode.TRIAL != operationMode && License.OperationMode.compare(operationMode, functionLicense) < 0) {
                    // non-compliant license
                    final VerificationException ex = expectThrows(VerificationException.class, () -> analyze(builder, operationMode));
                    assertThat(ex.getMessage(), containsString("current license is non-compliant for function [dummy()]"));
                } else {
                    // compliant license
                    assertNotNull(analyze(builder, operationMode));
                }
            }
        }
    }

    private LogicalPlan analyze(EsqlFunctionRegistry.FunctionBuilder builder, License.OperationMode operationMode) {
        final FunctionDefinition def = EsqlFunctionRegistry.def(DummyAggregateFunction.class, builder, "dummy");
        final EsqlFunctionRegistry registry = new EsqlFunctionRegistry(def) {
            @Override
            public EsqlFunctionRegistry snapshotRegistry() {
                return this;
            }
        };
        return analyzer(registry, operationMode).analyze(parser.createStatement(esql));
    }

    private static Analyzer analyzer(EsqlFunctionRegistry registry, License.OperationMode operationMode) {
        return new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, registry, analyzerDefaultMapping(), defaultEnrichResolution()),
            new Verifier(new Metrics(new EsqlFunctionRegistry()), getLicenseState(operationMode))
        );
    }

    private static XPackLicenseState getLicenseState(License.OperationMode operationMode) {
        final TestUtils.UpdatableLicenseState licenseState = new TestUtils.UpdatableLicenseState();
        licenseState.update(new XPackLicenseStatus(operationMode, true, null));
        return licenseState;
    }

    private static class DummyAggregateFunction extends Function {

        private final LicensedFeature licensedFeature;

        protected DummyAggregateFunction(Source source, List<Expression> children, LicensedFeature licensedFeature) {
            super(source, children);
            this.licensedFeature = licensedFeature;
        }

        @Override
        public Predicate<XPackLicenseState> getLicenseChecker() {
            if (licensedFeature instanceof LicensedFeature.Momentary momentary) {
                return momentary::check;
            } else {
                return licensedFeature::checkWithoutTracking;
            }
        }

        @Override
        public DataType dataType() {
            return DataType.KEYWORD;
        }

        @Override
        public Expression replaceChildren(List<Expression> newChildren) {
            throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this);
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException();
        }
    }

}
