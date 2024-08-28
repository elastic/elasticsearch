/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexSerializationTests;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ByteSizeEqualsMatcher.byteSizeEquals;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.hamcrest.Matchers.equalTo;

public class ExchangeSinkExecSerializationTests extends ESTestCase {
    // TODO port this to AbstractPhysicalPlanSerializationTests when implementing NamedWriteable
    private Configuration config;

    public static Source randomSource() {
        int lineNumber = between(0, EXAMPLE_QUERY.length - 1);
        String line = EXAMPLE_QUERY[lineNumber];
        int offset = between(0, line.length() - 2);
        int length = between(1, line.length() - offset - 1);
        String text = line.substring(offset, offset + length);
        return new Source(lineNumber + 1, offset, text);
    }

    /**
     * Test the size of serializing a plan with many conflicts.
     * See {@link #testManyTypeConflicts(boolean, ByteSizeValue)} for more.
     */
    public void testManyTypeConflicts() throws IOException {
        testManyTypeConflicts(false, ByteSizeValue.ofBytes(1897374));
        /*
         * History:
         *  2.3mb - shorten error messages for UnsupportedAttributes #111973
         *  1.8mb - cache EsFields #112008
         */
    }

    /**
     * Test the size of serializing a plan with many conflicts.
     * See {@link #testManyTypeConflicts(boolean, ByteSizeValue)} for more.
     */
    public void testManyTypeConflictsWithParent() throws IOException {
        testManyTypeConflicts(true, ByteSizeValue.ofBytes(3271487));
        /*
         * History:
         *  2 gb+ - start
         * 43.3mb - Cache attribute subclasses #111447
         *  5.6mb - shorten error messages for UnsupportedAttributes #111973
         *  3.1mb - cache EsFields #112008
         */
    }

    /**
     * Test the size of serializing a plan with many conflicts. Callers of
     * this method intentionally use a very precise size for the serialized
     * data so a programmer making changes has to think when this size changes.
     * <p>
     *     In general, shrinking the over the wire size is great and the precise
     *     size should just ratchet downwards. Small upwards movement is fine so
     *     long as you understand why the change is happening and you think it's
     *     worth it for the data node request for a big index to grow.
     * </p>
     * <p>
     *     Large upwards movement in the size is not fine! Folks frequently make
     *     requests across large clusters with many fields and these requests can
     *     really clog up the network interface. Super large results here can make
     *     ESQL impossible to use at all for big mappings with many conflicts.
     * </p>
     */
    private void testManyTypeConflicts(boolean withParent, ByteSizeValue expected) throws IOException {
        EsIndex index = EsIndexSerializationTests.indexWithManyConflicts(withParent);
        List<Attribute> attributes = Analyzer.mappingAsAttributes(randomSource(), index.mapping());
        EsRelation relation = new EsRelation(randomSource(), index, attributes, IndexMode.STANDARD);
        Limit limit = new Limit(randomSource(), new Literal(randomSource(), 10, DataType.INTEGER), relation);
        Project project = new Project(randomSource(), limit, limit.output());
        FragmentExec fragmentExec = new FragmentExec(project);
        ExchangeSinkExec exchangeSinkExec = new ExchangeSinkExec(randomSource(), fragmentExec.output(), false, fragmentExec);
        try (
            BytesStreamOutput out = new BytesStreamOutput();
            PlanStreamOutput pso = new PlanStreamOutput(out, new PlanNameRegistry(), configuration())
        ) {
            pso.writePhysicalPlanNode(exchangeSinkExec);
            assertThat(ByteSizeValue.ofBytes(out.bytes().length()), byteSizeEquals(expected));
            try (
                PlanStreamInput psi = new PlanStreamInput(
                    out.bytes().streamInput(),
                    new PlanNameRegistry(),
                    getNamedWriteableRegistry(),
                    configuration()
                )
            ) {
                assertThat(psi.readPhysicalPlanNode(), equalTo(exchangeSinkExec));
            }
        }
    }

    private NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(PhysicalPlan.getNamedWriteables());
        entries.addAll(LogicalPlan.getNamedWriteables());
        entries.addAll(AggregateFunction.getNamedWriteables());
        entries.addAll(Expression.getNamedWriteables());
        entries.addAll(Attribute.getNamedWriteables());
        entries.addAll(Block.getNamedWriteables());
        entries.addAll(NamedExpression.getNamedWriteables());
        entries.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    private Configuration configuration() {
        return config;
    }

    private static final String[] EXAMPLE_QUERY = new String[] {
        "I am the very model of a modern Major-Gineral,",
        "I've information vegetable, animal, and mineral,",
        "I know the kings of England, and I quote the fights historical",
        "From Marathon to Waterloo, in order categorical;",
        "I'm very well acquainted, too, with matters mathematical,",
        "I understand equations, both the simple and quadratical,",
        "About binomial theorem I'm teeming with a lot o' news,",
        "With many cheerful facts about the square of the hypotenuse." };

    @Before
    public void initConfig() {
        config = randomConfiguration(String.join("\n", EXAMPLE_QUERY), Map.of());
    }
}
