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
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.telemetry.Metrics;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzerDefaultMapping;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultEnrichResolution;
import static org.hamcrest.Matchers.containsString;

public class CheckLicenseTests extends ESTestCase {

    private final EsqlParser parser = new EsqlParser();
    private final String esql = "from tests | eval license() | LIMIT 10";

    public void testLicense() {
        for (License.OperationMode functionLicense : License.OperationMode.values()) {
            final LicensedFeature functionLicenseFeature = random().nextBoolean()
                ? LicensedFeature.momentary("test", "license", functionLicense)
                : LicensedFeature.persistent("test", "license", functionLicense);
            for (License.OperationMode operationMode : License.OperationMode.values()) {
                if (License.OperationMode.TRIAL != operationMode && License.OperationMode.compare(operationMode, functionLicense) < 0) {
                    // non-compliant license
                    final VerificationException ex = expectThrows(
                        VerificationException.class,
                        () -> analyze(operationMode, functionLicenseFeature)
                    );
                    assertThat(ex.getMessage(), containsString("current license is non-compliant for [license()]"));
                    assertThat(ex.getMessage(), containsString("current license is non-compliant for [LicensedLimit]"));
                } else {
                    // compliant license
                    assertNotNull(analyze(operationMode, functionLicenseFeature));
                }
            }
        }
    }

    private LogicalPlan analyze(License.OperationMode operationMode, LicensedFeature functionLicenseFeature) {
        final EsqlFunctionRegistry.FunctionBuilder builder = (source, expression, cfg) -> new LicensedFunction(
            source,
            functionLicenseFeature
        );
        final FunctionDefinition def = EsqlFunctionRegistry.def(LicensedFunction.class, builder, "license");
        final EsqlFunctionRegistry registry = new EsqlFunctionRegistry(def) {
            @Override
            public EsqlFunctionRegistry snapshotRegistry() {
                return this;
            }
        };

        var plan = parser.createStatement(esql);
        plan = plan.transformDown(
            Limit.class,
            l -> Objects.equals(l.limit().fold(FoldContext.small()), 10)
                ? new LicensedLimit(l.source(), l.limit(), l.child(), functionLicenseFeature)
                : l
        );
        return analyzer(registry, operationMode).analyze(plan);
    }

    private static Analyzer analyzer(EsqlFunctionRegistry registry, License.OperationMode operationMode) {
        return new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                registry,
                analyzerDefaultMapping(),
                defaultEnrichResolution(),
                emptyInferenceResolution()
            ),
            new Verifier(List.of(), new Metrics(new EsqlFunctionRegistry()), getLicenseState(operationMode))
        );
    }

    private static XPackLicenseState getLicenseState(License.OperationMode operationMode) {
        final TestUtils.UpdatableLicenseState licenseState = new TestUtils.UpdatableLicenseState();
        licenseState.update(new XPackLicenseStatus(operationMode, true, null));
        return licenseState;
    }

    // It needs to be public because we run validation on it via reflection in org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests.
    // This test prevents to add the license as constructor parameter too.
    public static class LicensedFunction extends Function implements LicenseAware {

        private final LicensedFeature licensedFeature;

        public LicensedFunction(Source source, LicensedFeature licensedFeature) {
            super(source, List.of());
            this.licensedFeature = licensedFeature;
        }

        @Override
        public boolean licenseCheck(XPackLicenseState state) {
            return checkLicense(state, licensedFeature);
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
            return NodeInfo.create(this, LicensedFunction::new, licensedFeature);
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

    public static class LicensedLimit extends Limit implements LicenseAware {

        private final LicensedFeature licensedFeature;

        public LicensedLimit(Source source, Expression limit, LogicalPlan child, LicensedFeature licensedFeature) {
            super(source, limit, child);
            this.licensedFeature = licensedFeature;
        }

        @Override
        public boolean licenseCheck(XPackLicenseState state) {
            return checkLicense(state, licensedFeature);
        }

        @Override
        public Limit replaceChild(LogicalPlan newChild) {
            return new LicensedLimit(source(), limit(), newChild, licensedFeature);
        }

        @Override
        protected NodeInfo<Limit> info() {
            return NodeInfo.create(this, LicensedLimit::new, limit(), child(), licensedFeature);
        }

        @Override
        public String sourceText() {
            return "LicensedLimit";
        }
    }

    private static boolean checkLicense(XPackLicenseState state, LicensedFeature licensedFeature) {
        return licensedFeature instanceof LicensedFeature.Momentary momentary
            ? momentary.check(state)
            : licensedFeature.checkWithoutTracking(state);
    }
}
