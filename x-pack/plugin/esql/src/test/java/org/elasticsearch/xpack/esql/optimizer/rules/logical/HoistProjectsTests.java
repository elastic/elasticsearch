/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.IpLocation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegisteredDomain;
import org.elasticsearch.xpack.esql.plan.logical.UriParts;
import org.elasticsearch.xpack.esql.plan.logical.UserAgent;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.DenseVector;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.rule.RuleExecutionException;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class HoistProjectsTests extends AbstractLogicalPlanOptimizerTests {

    public HoistProjectsTests(VersionMode versionMode) {
        super(versionMode);
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#8857, emp_no{f}#8851, first_name{f}#8852, gender{f}#8853, hire_date{f}#8858, job{f}#8859, job.raw{f}#8860,
     * languages{f}#8854 AS language_code#8849, last_name{f}#8855, long_noidx{f}#8861, salary{f}#8856, language_name{f}#8863]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Join[LEFT,[languages{f}#8854],[language_code{f}#8862],null,ANY]
     *     |_EsRelation[test][_meta_field{f}#8857, emp_no{f}#8851, first_name{f}#..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#8862, language_name{f}#8863]
     * }
     */
    public void testJoinOverRenamedKeyIsHoisted() {
        Join join = hoistedPast("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            """, Join.class);

        assertThat(Expressions.names(join.config().leftFields()), contains("languages"));
        as(join.left(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[emp_no{f}#8896, language_code{r}#8892, language_name{f}#8908]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Join[LEFT,[language_code{r}#8892],[language_code{f}#8907],null,ANY]
     *     |_Eval[[languages{f}#8899 + 1[INTEGER] AS language_code#8892]]
     *     | \_EsRelation[test][_meta_field{f}#8902, emp_no{f}#8896, first_name{f}#..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#8907, language_name{f}#8908]
     * }
     */
    public void testJoinOverKeepIsHoisted() {
        Join join = hoistedPast("""
            FROM test
            | EVAL language_code = languages + 1
            | KEEP emp_no, language_code
            | LOOKUP JOIN languages_lookup ON language_code
            """, Join.class);

        assertThat(Expressions.names(join.config().leftFields()), contains("language_code"));
        as(join.left(), Eval.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[emp_no{f}#9075, $$first_name$temp_name$9097{r$}#9098 AS original_first_name#9073, _meta_field{f}#9092, first_name{f}#9087,
     * gender{f}#9088, hire_date{f}#9093, job{f}#9094, job.raw{f}#9095, languages{f}#9089, last_name{f}#9090, long_noidx{f}#9096,
     * salary{f}#9091]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Join[LEFT,[emp_no{f}#9075],[emp_no{f}#9086],null,ANY]
     *     |_Eval[[first_name{f}#9076 AS $$first_name$temp_name$9097#9098]]
     *     | \_EsRelation[test][_meta_field{f}#9081, emp_no{f}#9075, first_name{f}#..]
     *     \_EsRelation[test_lookup][LOOKUP][_meta_field{f}#9092, emp_no{f}#9086, first_name{f}#..]
     * }
     */
    public void testJoinShadowingARenamedColumnIsHoistedWithATemporaryName() {
        Join join = hoistedPast("""
            FROM test
            | RENAME first_name AS original_first_name
            | LOOKUP JOIN test_lookup ON emp_no
            """, Join.class);

        Eval temporaryNames = as(join.left(), Eval.class);
        assertThat(Expressions.names(temporaryNames.fields()), contains(startsWith("$$first_name")));
    }

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_InlineJoin[LEFT,[language_code{r}#6859],[language_code{r}#6859]]
     *   |_Project[[_meta_field{f}#6870, emp_no{f}#6864, first_name{f}#6865, gender{f}#6866, hire_date{f}#6871, job{f}#6872,
     * job.raw{f}#6873, languages{f}#6867 AS language_code#6859, last_name{f}#6868, long_noidx{f}#6874, salary{f}#6869]]
     *   | \_EsRelation[test][_meta_field{f}#6870, emp_no{f}#6864, first_name{f}#..]
     *   \_Aggregate[[language_code{r}#6859],[MAX(salary{f}#6869,true[BOOLEAN],PT0S[TIME_DURATION]) AS max_salary#6862,
     * language_code{r}#6859]]
     *     \_StubRelation[[_meta_field{f}#6870, emp_no{f}#6864, first_name{f}#6865, gender{f}#6866, hire_date{f}#6871, job{f}#6872,
     * job.raw{f}#6873, language_code{r}#6859, last_name{f}#6868, long_noidx{f}#6874, salary{f}#6869]]
     * }
     */
    public void testProjectIsNotHoistedPastInlineJoin() {
        LogicalPlan plan = new HoistProjects().apply(substituted("""
            FROM test
            | RENAME languages AS language_code
            | INLINE STATS max_salary = MAX(salary) BY language_code
            """));

        var inlineJoin = plan.collect(InlineJoin.class).getFirst();
        as(inlineJoin.left(), Project.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#5451, emp_no{f}#5445, first_name{f}#5446 AS b#5443, gender{f}#5447, hire_date{f}#5452, job{f}#5453,
     * job.raw{f}#5454, languages{f}#5448, last_name{f}#5449, long_noidx{f}#5455, salary{f}#5450]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Eval[[1[INTEGER] AS x#5440]]
     *     \_EsRelation[test][_meta_field{f}#5451, emp_no{f}#5445, first_name{f}#..]
     * }
     */
    public void testProjectOverProjectIsCombined() {
        LogicalPlan substituted = substituted("""
            FROM test
            | RENAME first_name AS a
            | EVAL x = 1
            | RENAME a AS b
            | DROP x
            """);
        assertTrue(hasProjectChild(substituted, Project.class));

        LogicalPlan plan = new HoistProjects().apply(substituted);

        var project = as(plan, Project.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        assertThat(plan.output(), equalTo(substituted.output()));
        Alias b = (Alias) project.projections().stream().filter(p -> p.name().equals("b")).findFirst().orElseThrow();
        assertThat(Expressions.name(b.child()), equalTo("first_name"));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#5537, emp_no{f}#5531, first_name{f}#5532, gender{f}#5533, hire_date{f}#5538, job{f}#5539, job.raw{f}#5540,
     * languages{f}#5534, last_name{f}#5535, long_noidx{f}#5541, salary{f}#5536 AS s#5527, double_s{r}#5530]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Eval[[salary{f}#5536 * 2[INTEGER] AS double_s#5530]]
     *     \_EsRelation[test][_meta_field{f}#5537, emp_no{f}#5531, first_name{f}#..]
     * }
     */
    public void testEvalOverRenameIsPushedBelowTheProject() {
        Eval eval = hoistedPast("""
            FROM test
            | RENAME salary AS s
            | EVAL double_s = s * 2
            """, Eval.class);

        assertThat(eval.references().names(), equalTo(Set.of("salary")));
        as(eval.child(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#6195, emp_no{f}#6189, first_name{f}#6190, gender{f}#6191, hire_date{f}#6196, job{f}#6197, job.raw{f}#6198,
     * languages{f}#6192, last_name{f}#6193, long_noidx{f}#6199, salary{f}#6194 AS s#6185, $$salary$temp_name$6200{r}#6201 AS salary#6188]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Eval[[salary{f}#6194 + 1[INTEGER] AS $$salary$temp_name$6200#6201]]
     *     \_EsRelation[test][_meta_field{f}#6195, emp_no{f}#6189, first_name{f}#..]
     * }
     */
    public void testEvalShadowingARenamedColumnGetsATemporaryName() {
        Eval eval = hoistedPast("""
            FROM test
            | RENAME salary AS s
            | EVAL salary = s + 1
            """, Eval.class);

        assertThat(Expressions.names(eval.fields()), contains(startsWith("$$salary")));
    }

    /**
     * {@snippet lang="text":
     * Project[[emp_no{f}#6911, salary{f}#6916]]
     * \_Limit[10000[INTEGER],false,false]
     *   \_Limit[5[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#6917, emp_no{f}#6911, first_name{f}#..]
     * }
     */
    public void testLimitOverKeepIsSwapped() {
        LogicalPlan plan = hoistedOnce("""
            FROM test
            | KEEP emp_no, salary
            | LIMIT 5
            """, Limit.class);

        Limit limit = plan.collect(Limit.class)
            .stream()
            .filter(l -> as(l.limit(), Literal.class).value().equals(5))
            .findFirst()
            .orElseThrow();
        as(limit.child(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[field{f}#8928]]
     * \_Limit[10[INTEGER],true,true]
     *   \_EsRelation[test][field{f}#8928]
     * }
     */
    public void testLimitKeepsItsDuplicatedAndLocalFlags() {
        EsRelation relation = relation();
        Project project = new Project(EMPTY, relation, relation.output());

        LogicalPlan plan = hoistedOnce(new Limit(EMPTY, new Literal(EMPTY, 10, DataType.INTEGER), project, true, true), Limit.class);

        Limit limit = as(as(plan, Project.class).child(), Limit.class);
        assertTrue(limit.duplicated());
        assertTrue(limit.local());
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#6156, emp_no{f}#6150, first_name{f}#6151 AS fn#6148, gender{f}#6152, hire_date{f}#6157, job{f}#6158,
     * job.raw{f}#6159, languages{f}#6153, last_name{f}#6154, long_noidx{f}#6160, salary{f}#6155]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[first_name{f}#6151 == Georgi[KEYWORD]]
     *     \_EsRelation[test][_meta_field{f}#6156, emp_no{f}#6150, first_name{f}#..]
     * }
     */
    public void testFilterOverRenameIsPushedBelowTheProject() {
        Filter filter = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | WHERE fn == "Georgi"
            """, Filter.class);

        assertThat(filter.condition().references().names(), equalTo(Set.of("first_name")));
        as(filter.child(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[emp_no{f}#8950, salary{f}#8955]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[salary{f}#8955 > 40000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#8956, emp_no{f}#8950, first_name{f}#..]
     * }
     */
    public void testFilterOverKeepIsPushedBelowTheProject() {
        Filter filter = hoistedPast("""
            FROM test
            | KEEP emp_no, salary
            | WHERE salary > 40000
            """, Filter.class);

        assertThat(filter.condition().references().names(), equalTo(Set.of("salary")));
        as(filter.child(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#5431, emp_no{f}#5425, first_name{f}#5426 AS fn#5421, gender{f}#5427, hire_date{f}#5432, job{f}#5433,
     * job.raw{f}#5434, languages{f}#5428, last_name{f}#5429, long_noidx{f}#5435, salary{f}#5430, head{r}#5422, tail{r}#5423]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Dissect[first_name{f}#5426,Parser[pattern=%{head} %{tail}, appendSeparator=, parser=DissectParser],[head{r}#5422, tail{r}#5423]]
     *     \_EsRelation[test][_meta_field{f}#5431, emp_no{f}#5425, first_name{f}#..]
     * }
     */
    public void testDissectOverRenameIsPushedBelowTheProject() {
        Dissect dissect = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | DISSECT fn "%{head} %{tail}"
            """, Dissect.class);

        assertThat(dissect.input().references().names(), equalTo(Set.of("first_name")));
        assertThat(Expressions.names(dissect.extractedFields()), contains("head", "tail"));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#8940, emp_no{f}#8934, first_name{f}#8935 AS fn#8931, gender{f}#8936, hire_date{f}#8941, job{f}#8942,
     * job.raw{f}#8943, languages{f}#8937, last_name{f}#8938, long_noidx{f}#8944, salary{f}#8939,
     * $$first_name$temp_name$8945{r}#8946 AS first_name#8932]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Dissect[first_name{f}#8935,Parser[pattern=%{first_name}, appendSeparator=,
     * parser=DissectParser],[$$first_name$temp_name$8945{r}#8946]]
     *     \_EsRelation[test][_meta_field{f}#8940, emp_no{f}#8934, first_name{f}#..]
     * }
     */
    public void testDissectShadowingARenamedColumnGetsATemporaryName() {
        Dissect dissect = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | DISSECT fn "%{first_name}"
            """, Dissect.class);

        assertThat(Expressions.names(dissect.extractedFields()), contains(startsWith("$$first_name")));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#6214, emp_no{f}#6208, first_name{f}#6209 AS fn#6204, gender{f}#6210, hire_date{f}#6215, job{f}#6216,
     * job.raw{f}#6217, languages{f}#6211, last_name{f}#6212, long_noidx{f}#6218, salary{f}#6213, word{r}#6207]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Grok[first_name{f}#6209,Parser[pattern=%{WORD:word}],[word{r}#6207]]
     *     \_EsRelation[test][_meta_field{f}#6214, emp_no{f}#6208, first_name{f}#..]
     * }
     */
    public void testGrokOverRenameIsPushedBelowTheProject() {
        Grok grok = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | GROK fn "%{WORD:word}"
            """, Grok.class);

        assertThat(grok.input().references().names(), equalTo(Set.of("first_name")));
        assertThat(Expressions.names(grok.extractedFields()), contains("word"));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#8921, emp_no{f}#8915, first_name{f}#8916 AS fn#8911, gender{f}#8917, hire_date{f}#8922, job{f}#8923,
     * job.raw{f}#8924, languages{f}#8918, last_name{f}#8919, long_noidx{f}#8925, salary{f}#8920,
     * $$first_name$temp_name$8926{r}#8927 AS first_name#8914]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Grok[first_name{f}#8916,Parser[pattern=%{WORD:first_name}],[$$first_name$temp_name$8926{r}#8927]]
     *     \_EsRelation[test][_meta_field{f}#8921, emp_no{f}#8915, first_name{f}#..]
     * }
     */
    public void testGrokShadowingARenamedColumnGetsATemporaryName() {
        Grok grok = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | GROK fn "%{WORD:first_name}"
            """, Grok.class);

        assertThat(Expressions.names(grok.extractedFields()), contains(startsWith("$$first_name")));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#8839, emp_no{f}#8833, first_name{f}#8834, gender{f}#8835, hire_date{f}#8840, job{f}#8841, job.raw{f}#8842,
     * languages{f}#8836 AS lc#8830, last_name{f}#8837, long_noidx{f}#8843, salary{f}#8838, language_name{r}#8846]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Enrich[ANY,languages_idx[KEYWORD],languages{f}#8836,{"match":{"indices":[],"match_field":"id","enrich_fields":["language_code","l
     * anguage_name"]}},{=languages_idx},[language_name{r}#8846]]
     *     \_EsRelation[test][_meta_field{f}#8839, emp_no{f}#8833, first_name{f}#..]
     * }
     */
    public void testEnrichOverRenameIsPushedBelowTheProject() {
        Enrich enrich = hoistedPast("""
            FROM test
            | RENAME languages AS lc
            | ENRICH languages_idx ON lc WITH language_name
            """, Enrich.class);

        assertThat(enrich.matchField().name(), equalTo("languages"));
        as(enrich.child(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#5469, emp_no{f}#5463, first_name{f}#5464, gender{f}#5465, hire_date{f}#5470, job{f}#5471, job.raw{f}#5472,
     * languages{f}#5466 AS lc#5458, last_name{f}#5467, long_noidx{f}#5473, salary{f}#5468,
     * $$languages$temp_name$5478{r}#5479 AS languages#5477]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Enrich[ANY,languages_idx[KEYWORD],languages{f}#5466,{"match":{"indices":[],"match_field":"id","enrich_fields":["language_code","l
     * anguage_name"]}},{=languages_idx},[language_name{r}#5476 AS $$languages$temp_name$5478#5479]]
     *     \_EsRelation[test][_meta_field{f}#5469, emp_no{f}#5463, first_name{f}#..]
     * }
     */
    public void testEnrichShadowingARenamedColumnGetsATemporaryName() {
        Enrich enrich = hoistedPast("""
            FROM test
            | RENAME languages AS lc
            | ENRICH languages_idx ON lc WITH languages = language_name
            """, Enrich.class);

        assertThat(Expressions.names(enrich.generatedAttributes()), contains(startsWith("$$languages")));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#6811, emp_no{f}#6805, first_name{f}#6806 AS fn#6792, gender{f}#6807, hire_date{f}#6812, job{f}#6813,
     * job.raw{f}#6814, languages{f}#6808, last_name{f}#6809, long_noidx{f}#6815, salary{f}#6810, parts.domain{r}#6795,
     * parts.fragment{r}#6796, parts.path{r}#6797, parts.extension{r}#6798, parts.port{r}#6799, parts.query{r}#6800, parts.scheme{r}#6801,
     * parts.user_info{r}#6802, parts.username{r}#6803, parts.password{r}#6804]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_UriParts[first_name{f}#6806,[domain, fragment, path, extension, port, query, scheme, user_info, username,
     * password],[parts.domain{r}#6795, parts.fragment{r}#6796, parts.path{r}#6797, parts.extension{r}#6798, parts.port{r}#6799,
     * parts.query{r}#6800, parts.scheme{r}#6801, parts.user_info{r}#6802, parts.username{r}#6803, parts.password{r}#6804]]
     *     \_EsRelation[test][_meta_field{f}#6811, emp_no{f}#6805, first_name{f}#..]
     * }
     */
    public void testUriPartsOverRenameIsPushedBelowTheProject() {
        assumeTrue("requires uri_parts command capability", EsqlCapabilities.Cap.URI_PARTS_COMMAND.isEnabled());
        UriParts uriParts = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | URI_PARTS parts = fn
            """, UriParts.class);

        assertThat(uriParts.references().names(), equalTo(Set.of("first_name")));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#8978, emp_no{f}#8972, first_name{f}#8973 AS fn#8965, gender{f}#8974, hire_date{f}#8979, job{f}#8980,
     * job.raw{f}#8981, languages{f}#8975, last_name{f}#8976, long_noidx{f}#8982, salary{f}#8977, rd.domain{r}#8968,
     * rd.registered_domain{r}#8969, rd.top_level_domain{r}#8970, rd.subdomain{r}#8971]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_RegisteredDomain[first_name{f}#8973,[domain, registered_domain, top_level_domain, subdomain],[rd.domain{r}#8968,
     * rd.registered_domain{r}#8969, rd.top_level_domain{r}#8970, rd.subdomain{r}#8971]]
     *     \_EsRelation[test][_meta_field{f}#8978, emp_no{f}#8972, first_name{f}#..]
     * }
     */
    public void testRegisteredDomainOverRenameIsPushedBelowTheProject() {
        assumeTrue("requires registered_domain command capability", EsqlCapabilities.Cap.REGISTERED_DOMAIN_COMMAND.isEnabled());
        RegisteredDomain registeredDomain = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | REGISTERED_DOMAIN rd = fn
            """, RegisteredDomain.class);

        assertThat(registeredDomain.references().names(), equalTo(Set.of("first_name")));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#6939, emp_no{f}#6933, first_name{f}#6934 AS fn#6924, gender{f}#6935, hire_date{f}#6940, job{f}#6941,
     * job.raw{f}#6942, languages{f}#6936, last_name{f}#6937, long_noidx{f}#6943, salary{f}#6938, ua.name{r}#6927, ua.version{r}#6928,
     * ua.os.name{r}#6929, ua.os.version{r}#6930, ua.os.full{r}#6931, ua.device.name{r}#6932]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_UserAgent[first_name{f}#6934,[name, version, os.name, os.version, os.full, device.name],[ua.name{r}#6927, ua.version{r}#6928,
     * ua.os.name{r}#6929, ua.os.version{r}#6930, ua.os.full{r}#6931, ua.device.name{r}#6932],false,_default_]
     *     \_EsRelation[test][_meta_field{f}#6939, emp_no{f}#6933, first_name{f}#..]
     * }
     */
    public void testUserAgentOverRenameIsPushedBelowTheProject() {
        assumeTrue("requires user_agent command capability", EsqlCapabilities.Cap.USER_AGENT_COMMAND.isEnabled());
        UserAgent userAgent = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | USER_AGENT ua = fn
            """, UserAgent.class);

        assertThat(userAgent.references().names(), equalTo(Set.of("first_name")));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#8878, emp_no{f}#8872, first_name{f}#8873 AS fn#8869, gender{f}#8874, hire_date{f}#8879, job{f}#8880,
     * job.raw{f}#8881, languages{f}#8875, last_name{f}#8876, long_noidx{f}#8882, salary{f}#8877, g.country_iso_code{r}#8883,
     * g.country_name{r}#8884, g.continent_name{r}#8885, g.region_iso_code{r}#8886, g.region_name{r}#8887, g.city_name{r}#8888,
     * g.location{r}#8889]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_IpLocation[first_name{f}#8873,[country_iso_code, country_name, continent_name, region_iso_code, region_name, city_name,
     * location],[g.country_iso_code{r}#8883, g.country_name{r}#8884, g.continent_name{r}#8885, g.region_iso_code{r}#8886,
     * g.region_name{r}#8887, g.city_name{r}#8888, g.location{r}#8889],GeoLite2-City.mmdb,true]
     *     \_EsRelation[test][_meta_field{f}#8878, emp_no{f}#8872, first_name{f}#..]
     * }
     */
    public void testIpLocationOverRenameIsPushedBelowTheProject() {
        assumeTrue("requires ip_location command capability", EsqlCapabilities.Cap.IP_LOCATION_COMMAND.isEnabled());
        IpLocation ipLocation = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | IP_LOCATION g = fn
            """, IpLocation.class);

        assertThat(ipLocation.references().names(), equalTo(Set.of("first_name")));
    }

    /**
     * {@snippet lang="text":
     * Project[[field{f}#8864 AS renamed#8865, completion{r}#8866]]
     * \_Completion[inference-id[KEYWORD],1000[INTEGER],field{f}#8864,completion{r}#8866,MapExpression[[]],null]
     *   \_EsRelation[test][field{f}#8864]
     * }
     */
    public void testCompletionOverRenameIsPushedBelowTheProject() {
        EsRelation relation = relation();
        Attribute field = relation.output().getFirst();
        Alias renamed = new Alias(EMPTY, "renamed", field);
        Project project = new Project(EMPTY, relation, List.of(renamed));

        LogicalPlan plan = hoistedOnce(completion(project, renamed.toAttribute(), "completion"), Completion.class);

        Completion completion = as(as(plan, Project.class).child(), Completion.class);
        assertThat(completion.prompt(), equalTo(field));
        as(completion.child(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[field{f}#6161 AS renamed#6162, $$field$temp_name$6164{r}#6165 AS field#6163]]
     * \_Completion[inference-id[KEYWORD],1000[INTEGER],field{f}#6161,$$field$temp_name$6164{r}#6165,MapExpression[[]],null]
     *   \_EsRelation[test][field{f}#6161]
     * }
     */
    public void testCompletionShadowingARenamedColumnGetsATemporaryName() {
        EsRelation relation = relation();
        Attribute field = relation.output().getFirst();
        Alias renamed = new Alias(EMPTY, "renamed", field);
        Project project = new Project(EMPTY, relation, List.of(renamed));

        LogicalPlan plan = hoistedOnce(completion(project, renamed.toAttribute(), field.name()), Completion.class);

        Completion completion = as(as(plan, Project.class).child(), Completion.class);
        assertThat(completion.targetField().name(), startsWith("$$" + field.name()));
    }

    /**
     * {@snippet lang="text":
     * Project[[field{f}#9065 AS renamed#9066, _score{r}#9068]]
     * \_Rerank[.rerank-v1-elasticsearch[KEYWORD],1000[INTEGER],query[KEYWORD],[field{f}#9065 AS renamed#9067],_score{r}#9068,null]
     *   \_EsRelation[test][field{f}#9065]
     * }
     */
    public void testRerankOverRenameIsPushedBelowTheProject() {
        EsRelation relation = relation();
        Attribute field = relation.output().getFirst();
        Alias renamed = new Alias(EMPTY, "renamed", field);
        Project project = new Project(EMPTY, relation, List.of(renamed));
        Rerank rerankOverProject = new Rerank(
            EMPTY,
            project,
            new Literal(EMPTY, 1000, DataType.INTEGER),
            Literal.keyword(EMPTY, "query"),
            List.of(new Alias(EMPTY, "renamed", renamed.toAttribute())),
            referenceAttribute("_score", DataType.DOUBLE)
        );

        LogicalPlan plan = hoistedOnce(rerankOverProject, Rerank.class);

        Rerank rerank = as(as(plan, Project.class).child(), Rerank.class);
        assertThat(rerank.rerankFields().getFirst().child(), equalTo(field));
        as(rerank.child(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[field{f}#9069 AS renamed#9070]]
     * \_DENSE_VECTOR[.multilingual-e5-small-elasticsearch[KEYWORD],1000[INTEGER],[field{f}#9069],[],OutputNaming[explicitName=null,
     * suffix=_dense_vector],null,text,null,true]
     *   \_EsRelation[test][field{f}#9069]
     * }
     */
    public void testDenseVectorOverRenameIsPushedBelowTheProject() {
        EsRelation relation = relation();
        Attribute field = relation.output().getFirst();
        Alias renamed = new Alias(EMPTY, "renamed", field);
        Project project = new Project(EMPTY, relation, List.of(renamed));
        List<NamedExpression> fields = List.of(renamed.toAttribute());
        DenseVector denseVectorOverProject = new DenseVector(EMPTY, project, new Literal(EMPTY, 1000, DataType.INTEGER), fields);

        LogicalPlan plan = hoistedOnce(denseVectorOverProject, DenseVector.class);

        DenseVector denseVector = as(as(plan, Project.class).child(), DenseVector.class);
        assertThat(denseVector.references().names(), equalTo(Set.of(field.name())));
        as(denseVector.child(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#2322, emp_no{f}#2316, first_name{f}#2317 AS fn#2314, gender{f}#2318, hire_date{f}#2323, job{f}#2324,
     * job.raw{f}#2325, languages{f}#2319, last_name{f}#2320, long_noidx{f}#2326, salary{f}#2321]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[first_name{f}#2317,DESC,FIRST]]]
     *     \_EsRelation[test][_meta_field{f}#2322, emp_no{f}#2316, first_name{f}#..]
     * }
     */
    public void testOrderByOverRenameIsPushedBelowTheProject() {
        LogicalPlan before = defaultAnalyzer().query("""
            FROM test
            | RENAME first_name AS fn
            | SORT fn DESC NULLS FIRST
            """);

        LogicalPlan plan = hoistedOnce(before, OrderBy.class);
        OrderBy orderBy = plan.collect(OrderBy.class).getFirst();
        assertThat(orderBy.references().names(), equalTo(Set.of("first_name")));
        assertThat(orderBy.order().getFirst().direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(orderBy.order().getFirst().nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        as(orderBy.child(), EsRelation.class);
        assertThat(new HoistProjects().local().apply(before), equalTo(plan));
    }

    /**
     * {@snippet lang="text":
     * Project[[emp_no{f}#4659, salary{f}#4664]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[salary{f}#4664,DESC,LAST], Order[emp_no{f}#4659,ASC,FIRST]]]
     *     \_EsRelation[test][_meta_field{f}#4665, emp_no{f}#4659, first_name{f}#..]
     * }
     */
    public void testOrderByOverKeepIsPushedBelowTheProject() {
        LogicalPlan before = defaultAnalyzer().query("""
            FROM test
            | KEEP emp_no, salary
            | SORT salary DESC NULLS LAST, emp_no ASC NULLS FIRST
            """);

        LogicalPlan plan = hoistedOnce(before, OrderBy.class);
        OrderBy orderBy = plan.collect(OrderBy.class).getFirst();
        assertThat(orderBy.order().stream().map(o -> Expressions.name(o.child())).toList(), contains("salary", "emp_no"));
        assertThat(orderBy.order().stream().map(Order::direction).toList(), contains(Order.OrderDirection.DESC, Order.OrderDirection.ASC));
        assertThat(
            orderBy.order().stream().map(Order::nullsPosition).toList(),
            contains(Order.NullsPosition.LAST, Order.NullsPosition.FIRST)
        );
        as(orderBy.child(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#5439, emp_no{f}#5433, first_name{f}#5434, gender{f}#5435, hire_date{f}#5440, job{f}#5441, job.raw{f}#5442,
     * languages{f}#5436, last_name{f}#5437, long_noidx{f}#5443, salary{f}#5438 AS original_salary#5427,
     * $$salary$temp_name$5444{r}#5445 AS salary#5430]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[salary{f}#5438,DESC,LAST], Order[$$salary$temp_name$5444{r}#5445,ASC,FIRST]]]
     *     \_Eval[[salary{f}#5438 + 1[INTEGER] AS $$salary$temp_name$5444#5445]]
     *       \_EsRelation[test][_meta_field{f}#5439, emp_no{f}#5433, first_name{f}#..]
     * }
     */
    public void testOrderByOverRenamedAndShadowedColumns() {
        LogicalPlan before = defaultAnalyzer().query("""
            FROM test
            | RENAME salary AS original_salary
            | EVAL salary = original_salary + 1
            | SORT original_salary DESC NULLS LAST, salary ASC NULLS FIRST
            """);

        LogicalPlan plan = hoistedOnce(before, Eval.class);
        OrderBy orderBy = plan.collect(OrderBy.class).getFirst();
        Eval eval = as(orderBy.child(), Eval.class);
        Alias salary = eval.fields().getFirst();
        assertThat(salary.name(), startsWith("$$salary"));
        assertThat(salary.child().references().names(), equalTo(Set.of("salary")));
        assertThat(orderBy.order().getFirst().child(), equalTo(salary.child().references().iterator().next()));
        assertThat(orderBy.order().get(1).child(), equalTo(salary.toAttribute()));
        as(eval.child(), EsRelation.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#17, emp_no{f}#11, first_name{f}#12 AS name#9, gender{f}#13, hire_date{f}#18, job{f}#19, job.raw{f}#20,
     * languages{f}#14, last_name{f}#15, long_noidx{f}#21, salary{f}#16]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[first_name{f}#12,ASC,FIRST]]]
     *     \_OrderBy[[Order[first_name{f}#12,DESC,LAST]]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     * }
     */
    public void testOnePassHoistsTheProjectPastMultipleOrderBys() {
        LogicalPlan before = defaultAnalyzer().query("""
            FROM test
            | RENAME first_name AS fn
            | SORT fn DESC NULLS LAST
            | RENAME fn AS name
            | SORT name ASC NULLS FIRST
            """);

        LogicalPlan plan = hoistedOnce(before, OrderBy.class);
        List<OrderBy> sorts = plan.collect(OrderBy.class);
        assertThat(sorts, hasSize(2));
        OrderBy outer = sorts.getFirst();
        OrderBy inner = as(outer.child(), OrderBy.class);
        assertThat(outer.references().names(), equalTo(Set.of("first_name")));
        assertThat(inner.references(), equalTo(outer.references()));
        assertThat(outer.order().getFirst().direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(outer.order().getFirst().nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        assertThat(inner.order().getFirst().direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(inner.order().getFirst().nullsPosition(), equalTo(Order.NullsPosition.LAST));
        as(inner.child(), EsRelation.class);
        assertThat(new HoistProjects().apply(plan), equalTo(plan));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#5390, emp_no{f}#5384, first_name{f}#5385, gender{f}#5386, hire_date{f}#5391, job{f}#5392, job.raw{f}#5393,
     * languages{f}#5387, last_name{f}#5388, long_noidx{f}#5394, salary{f}#5389, $$language_name$temp_name$5401{r$}#5402 AS lang0#5363,
     * $$language_name$temp_name$5403{r$}#5404 AS lang1#5372, language_name{f}#5400 AS lang2#5381]]
     * \_Limit[10000[INTEGER],false,false]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_OrderBy[[Order[language_name{f}#5400,ASC,LAST]]]
     *       \_Join[LEFT,[language_code{r}#5377],[language_code{f}#5399],null,ANY]
     *         |_Eval[[language_name{f}#5398 AS $$language_name$temp_name$5403#5404]]
     *         | \_Eval[[languages{f}#5387 + 2[INTEGER] AS language_code#5377]]
     *         |   \_Limit[1000[INTEGER],false,false]
     *         |     \_OrderBy[[Order[language_name{f}#5398,ASC,LAST]]]
     *         |       \_Join[LEFT,[language_code{r}#5368],[language_code{f}#5397],null,ANY]
     *         |         |_Eval[[language_name{f}#5396 AS $$language_name$temp_name$5401#5402]]
     *         |         | \_Eval[[languages{f}#5387 + 1[INTEGER] AS language_code#5368]]
     *         |         |   \_Limit[1000[INTEGER],false,false]
     *         |         |     \_OrderBy[[Order[language_name{f}#5396,ASC,LAST]]]
     *         |         |       \_Join[LEFT,[language_code{r}#5359],[language_code{f}#5395],null,ANY]
     *         |         |         |_Eval[[languages{f}#5387 + 0[INTEGER] AS language_code#5359]]
     *         |         |         | \_EsRelation[test][_meta_field{f}#5390, emp_no{f}#5384, first_name{f}#..]
     *         |         |         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#5395, language_name{f}#5396]
     *         |         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#5397, language_name{f}#5398]
     *         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#5399, language_name{f}#5400]
     * }
     */
    public void testOneRunHoistsTheProjectPastOrderByAndLimitBetweenLookupBlocks() {
        LogicalPlan before = new SubstituteSurrogatePlans().apply(
            defaultAnalyzer().query(lookupBlocks(3, "\n| SORT lang%d\n| LIMIT 1000"))
        );

        LogicalPlan plan = hoistedOnce(before, OrderBy.class);
        assertThat(plan.collect(Join.class), hasSize(3));
        assertThat(plan.collect(Limit.class).stream().filter(l -> l.child() instanceof OrderBy).count(), equalTo(3L));
        List<OrderBy> sorts = plan.collect(OrderBy.class);
        assertThat(sorts, hasSize(3));
        for (OrderBy orderBy : sorts) {
            Join join = as(orderBy.child(), Join.class);
            Attribute sortField = as(orderBy.order().getFirst().child(), Attribute.class);
            assertThat(sortField.name(), equalTo("language_name"));
            assertTrue(join.outputSet().contains(sortField));
        }
        assertThat(Expressions.names(plan.output()), hasItems("lang0", "lang1", "lang2"));
    }

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[salary{f}#1363,DESC,LAST]]]
     *   \_EsRelation[test][_meta_field{f}#1364, emp_no{f}#1358, first_name{f}#..]
     * }
     */
    public void testOrderByWithoutProjectIsUnchanged() {
        LogicalPlan before = defaultAnalyzer().query("""
            FROM test
            | SORT salary DESC NULLS LAST
            """);

        assertThat(before.collect(Project.class), hasSize(0));
        assertSame(before, new HoistProjects().apply(before));
    }

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_MvExpand[fn{r}#6168,fn{r}#6182]
     *   \_Project[[_meta_field{f}#6177, emp_no{f}#6171, first_name{f}#6172 AS fn#6168, gender{f}#6173, hire_date{f}#6178, job{f}#6179,
     * job.raw{f}#6180, languages{f}#6174, last_name{f}#6175, long_noidx{f}#6181, salary{f}#6176]]
     *     \_EsRelation[test][_meta_field{f}#6177, emp_no{f}#6171, first_name{f}#..]
     * }
     */
    public void testProjectStaysBelowMvExpand() {
        LogicalPlan plan = new HoistProjects().apply(substituted("""
            FROM test
            | RENAME first_name AS fn
            | MV_EXPAND fn
            """));

        as(plan.collect(MvExpand.class).getFirst().child(), Project.class);
    }

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[MAX(s{r}#8813,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#8816]]
     *   \_Project[[_meta_field{f}#8823, emp_no{f}#8817, first_name{f}#8818, gender{f}#8819, hire_date{f}#8824, job{f}#8825,
     * job.raw{f}#8826, languages{f}#8820, last_name{f}#8821, long_noidx{f}#8827, salary{f}#8822 AS s#8813]]
     *     \_EsRelation[test][_meta_field{f}#8823, emp_no{f}#8817, first_name{f}#..]
     * }
     */
    public void testProjectStaysBelowAggregate() {
        LogicalPlan plan = new HoistProjects().apply(substituted("""
            FROM test
            | RENAME salary AS s
            | STATS m = MAX(s)
            """));

        as(plan.collect(Aggregate.class).getFirst().child(), Project.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#6847, emp_no{f}#6841, first_name{f}#6842 AS fn#6818, gender{f}#6843, hire_date{f}#6848, job{f}#6849,
     * job.raw{f}#6850, languages{f}#6844 AS lc#6830, last_name{f}#6845, long_noidx{f}#6851, salary{f}#6846, fn_length{r}#6822,
     * head{r}#6823, word{r}#6827, language_name{f}#6853 AS looked_up#6839]]
     * \_Limit[10000[INTEGER],false,false]
     *   \_Join[LEFT,[languages{f}#6844],[language_code{f}#6852],null,ANY]
     *     |_Limit[10[INTEGER],false,false]
     *     | \_Enrich[ANY,languages_idx[KEYWORD],languages{f}#6844,{"match":{"indices":[],"match_field":"id","enrich_fields":["language_code
     * ","language_name"]}},{=languages_idx},[language_name{r}#6856]]
     *     |   \_Grok[first_name{f}#6842,Parser[pattern=%{WORD:word}],[word{r}#6827]]
     *     |     \_Dissect[first_name{f}#6842,Parser[pattern=%{head}, appendSeparator=, parser=DissectParser],[head{r}#6823]]
     *     |       \_Eval[[LENGTH(first_name{f}#6842) AS fn_length#6822]]
     *     |         \_Filter[ISNOTNULL(first_name{f}#6842)]
     *     |           \_EsRelation[test][_meta_field{f}#6847, emp_no{f}#6841, first_name{f}#..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#6852, language_name{f}#6853]
     * }
     */
    public void testOnePassHoistsTheProjectPastEverySupportedNode() {
        hoistedOnce("""
            FROM test
            | RENAME first_name AS fn
            | WHERE fn IS NOT NULL
            | EVAL fn_length = LENGTH(fn)
            | DISSECT fn "%{head}"
            | GROK fn "%{WORD:word}"
            | RENAME languages AS lc
            | ENRICH languages_idx ON lc WITH language_name
            | LIMIT 10
            | EVAL language_code = lc
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS looked_up
            | DROP language_code
            """, Filter.class);
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#5510, emp_no{f}#5504, first_name{f}#5505, gender{f}#5506, hire_date{f}#5511, job{f}#5512, job.raw{f}#5513,
     * languages{f}#5507, last_name{f}#5508, long_noidx{f}#5514, salary{f}#5509, $$language_name$temp_name$5521{r$}#5522 AS lang0#5486,
     * $$language_name$temp_name$5523{r$}#5524 AS lang1#5494, language_name{f}#5520 AS lang2#5502]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Join[LEFT,[language_code{r}#5498],[language_code{f}#5519],null,ANY]
     *     |_Eval[[language_name{f}#5518 AS $$language_name$temp_name$5523#5524]]
     *     | \_Eval[[languages{f}#5507 + 2[INTEGER] AS language_code#5498]]
     *     |   \_Join[LEFT,[language_code{r}#5490],[language_code{f}#5517],null,ANY]
     *     |     |_Eval[[language_name{f}#5516 AS $$language_name$temp_name$5521#5522]]
     *     |     | \_Eval[[languages{f}#5507 + 1[INTEGER] AS language_code#5490]]
     *     |     |   \_Join[LEFT,[language_code{r}#5482],[language_code{f}#5515],null,ANY]
     *     |     |     |_Eval[[languages{f}#5507 + 0[INTEGER] AS language_code#5482]]
     *     |     |     | \_EsRelation[test][_meta_field{f}#5510, emp_no{f}#5504, first_name{f}#..]
     *     |     |     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#5515, language_name{f}#5516]
     *     |     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#5517, language_name{f}#5518]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#5519, language_name{f}#5520]
     * }
     */
    public void testOneRunHoistsTheProjectAboveEveryLookupBlock() {
        LogicalPlan plan = new HoistProjects().apply(substituted(lookupBlocks(3)));

        var project = as(plan, Project.class);
        as(project.child(), Limit.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        List<Join> joins = plan.collect(Join.class);
        assertThat(joins, hasSize(3));
        assertThat(joins.stream().map(Join::left).toList(), everyItem(not(instanceOf(Project.class))));
        assertThat(Expressions.names(project.projections()), hasItems("lang0", "lang1", "lang2"));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#6898, emp_no{f}#6892, first_name{f}#6893, gender{f}#6894, hire_date{f}#6899, job{f}#6900, job.raw{f}#6901,
     * languages{f}#6895, last_name{f}#6896, long_noidx{f}#6902, salary{f}#6897, $$language_name$temp_name$6907{r$}#6908 AS lang#6881,
     * upper{r}#6884, language_code{r}#6887, language_name{f}#6906 AS next_lang#6891]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Join[LEFT,[language_code{r}#6887],[language_code{f}#6905],null,ANY]
     *     |_Eval[[language_name{f}#6904 AS $$language_name$temp_name$6907#6908]]
     *     | \_Eval[[languages{f}#6895 + 1[INTEGER] AS language_code#6887]]
     *     |   \_Eval[[TOUPPER(language_name{f}#6904) AS upper#6884]]
     *     |     \_Join[LEFT,[languages{f}#6895],[language_code{f}#6903],null,ANY]
     *     |       |_EsRelation[test][_meta_field{f}#6898, emp_no{f}#6892, first_name{f}#..]
     *     |       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#6903, language_name{f}#6904]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#6905, language_name{f}#6906]
     * }
     */
    public void testOneRunHoistsTheProjectPastEvalsBetweenLookupBlocks() {
        LogicalPlan plan = new HoistProjects().apply(substituted("""
            FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | EVAL upper = TO_UPPER(lang)
            | EVAL language_code = languages + 1
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS next_lang
            """));

        as(plan, Project.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        Join upperJoin = plan.collect(Join.class).getFirst();
        var eval = as(upperJoin.left(), Eval.class);
        assertThat(eval.collect(Project.class), hasSize(0));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#9016, emp_no{f}#9010, first_name{f}#9011, gender{f}#9012, hire_date{f}#9017, job{f}#9018, job.raw{f}#9019,
     * languages{f}#9013, last_name{f}#9014, long_noidx{f}#9020, salary{f}#9015, $$language_name$temp_name$9027{r$}#9028 AS lang0#8989,
     * $$language_name$temp_name$9029{r$}#9030 AS lang1#8998, language_name{f}#9026 AS lang2#9007]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[ISNOTNULL(language_name{f}#9026)]
     *     \_Join[LEFT,[language_code{r}#9003],[language_code{f}#9025],null,ANY]
     *       |_Eval[[language_name{f}#9024 AS $$language_name$temp_name$9029#9030]]
     *       | \_Eval[[languages{f}#9013 + 2[INTEGER] AS language_code#9003]]
     *       |   \_Filter[ISNOTNULL(language_name{f}#9024)]
     *       |     \_Join[LEFT,[language_code{r}#8994],[language_code{f}#9023],null,ANY]
     *       |       |_Eval[[language_name{f}#9022 AS $$language_name$temp_name$9027#9028]]
     *       |       | \_Eval[[languages{f}#9013 + 1[INTEGER] AS language_code#8994]]
     *       |       |   \_Filter[ISNOTNULL(language_name{f}#9022)]
     *       |       |     \_Join[LEFT,[language_code{r}#8985],[language_code{f}#9021],null,ANY]
     *       |       |       |_Eval[[languages{f}#9013 + 0[INTEGER] AS language_code#8985]]
     *       |       |       | \_EsRelation[test][_meta_field{f}#9016, emp_no{f}#9010, first_name{f}#..]
     *       |       |       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#9021, language_name{f}#9022]
     *       |       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#9023, language_name{f}#9024]
     *       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#9025, language_name{f}#9026]
     * }
     */
    public void testSecondApplicationChangesNothing() {
        LogicalPlan once = new HoistProjects().apply(substituted(lookupBlocks(3, "\n| WHERE lang%d IS NOT NULL")));

        assertThat(new HoistProjects().apply(once), equalTo(once));
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{f}#9054, emp_no{f}#9048, first_name{f}#9049 AS fn#9033, gender{f}#9050, hire_date{f}#9055, job{f}#9056,
     * job.raw{f}#9057, languages{f}#9051, last_name{f}#9052, long_noidx{f}#9058, salary{f}#9053, head{r}#9035, language_code{r}#9039,
     * language_name{f}#9060 AS lang#9043, enriched{r}#9064]]
     * \_Limit[10000[INTEGER],false,false]
     *   \_Limit[10[INTEGER],false,false]
     *     \_Enrich[ANY,languages_idx[KEYWORD],languages{f}#9051,{"match":{"indices":[],"match_field":"id","enrich_fields":["language_code",
     * "language_name"]}},{=languages_idx},[language_name{r}#9063 AS enriched#9064]]
     *       \_Join[LEFT,[language_code{r}#9039],[language_code{f}#9059],null,ANY]
     *         |_Eval[[languages{f}#9051 + 1[INTEGER] AS language_code#9039]]
     *         | \_Dissect[first_name{f}#9049,Parser[pattern=%{head}, appendSeparator=, parser=DissectParser],[head{r}#9035]]
     *         |   \_Filter[ISNOTNULL(first_name{f}#9049)]
     *         |     \_EsRelation[test][_meta_field{f}#9054, emp_no{f}#9048, first_name{f}#..]
     *         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#9059, language_name{f}#9060]
     * }
     */
    public void testLocalVariantHoistsTheSameWay() {
        LogicalPlan substituted = substituted("""
            FROM test
            | RENAME first_name AS fn
            | WHERE fn IS NOT NULL
            | DISSECT fn "%{head}"
            | EVAL language_code = languages + 1
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | ENRICH languages_idx ON languages WITH enriched = language_name
            | LIMIT 10
            """);

        assertThat(new HoistProjects().local(), instanceOf(HoistProjects.class));
        assertThat(new HoistProjects().local().apply(substituted), equalTo(new HoistProjects().apply(substituted)));
    }

    public void testOperatorBatchRunsDoNotGrowWithTheNumberOfLookupBlocks() {
        assertThat(operatorRunsToConverge(lookupBlocks(30)), equalTo(operatorRunsToConverge(lookupBlocks(3))));
    }

    public void testOperatorBatchRunsDoNotGrowWithFiltersOnLookedUpColumns() {
        String block = "\n| WHERE lang%d IS NOT NULL";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    /**
     * Plan for the three-block case (also compared with 30 blocks):
     * {@snippet lang="text":
     * Project[[_meta_field{f}#1334, emp_no{f}#1328, first_name{f}#1329, gender{f}#1330, hire_date{f}#1335, job{f}#1336, job.raw{f}#1337,
     * languages{f}#1331, last_name{f}#1332, long_noidx{f}#1338, salary{f}#1333, $$language_name$temp_name$1353{r$}#1354 AS lang0#1307,
     * $$language_name$temp_name$1355{r$}#1356 AS lang1#1316, language_name{f}#1344 AS lang2#1325]]
     * \_TopN[[Order[language_name{f}#1344,ASC,LAST]],1000[INTEGER],false]
     *   \_Join[LEFT,[language_code{r}#1321],[language_code{f}#1343],null,ANY]
     *     |_Eval[[languages{f}#1331 + 2[INTEGER] AS language_code#1321, language_name{f}#1342 AS $$language_name$temp_name$1355#1356]]
     *     | \_TopN[[Order[language_name{f}#1342,ASC,LAST]],1000[INTEGER],false]
     *     |   \_Join[LEFT,[language_code{r}#1312],[language_code{f}#1341],null,ANY]
     *     |     |_Eval[[languages{f}#1331 + 1[INTEGER] AS language_code#1312, language_name{f}#1340 AS
     * $$language_name$temp_name$1353#1354]]
     *     |     | \_TopN[[Order[language_name{f}#1340,ASC,LAST]],1000[INTEGER],false]
     *     |     |   \_Join[LEFT,[language_code{r}#1303],[language_code{f}#1339],null,ANY]
     *     |     |     |_Eval[[languages{f}#1331 + 0[INTEGER] AS language_code#1303]]
     *     |     |     | \_EsRelation[test][_meta_field{f}#1334, emp_no{f}#1328, first_name{f}#..]
     *     |     |     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1339, language_name{f}#1340]
     *     |     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1341, language_name{f}#1342]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1343, language_name{f}#1344]
     * }
     */
    public void testOperatorBatchRunsDoNotGrowWithSortAndLimitOnLookedUpColumns() {
        String block = "\n| SORT lang%d\n| LIMIT 1000";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testOperatorBatchRunsDoNotGrowWithDissectBetweenLookupBlocks() {
        String block = "\n| DISSECT first_name \"%%{part%d}\"";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testOperatorBatchRunsDoNotGrowWithUriPartsBetweenLookupBlocks() {
        assumeTrue("requires uri_parts command capability", EsqlCapabilities.Cap.URI_PARTS_COMMAND.isEnabled());
        String block = "\n| URI_PARTS uri%d = first_name";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testOperatorBatchRunsDoNotGrowWithEnrichBetweenLookupBlocks() {
        String block = "\n| ENRICH languages_idx ON languages WITH enriched%d = language_name";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testLookupChainLongerThanTheRuleExecutionLimitOptimizes() {
        int blocks = 110;
        String query = lookupBlocks(blocks);
        LogicalPlan analyzed = defaultAnalyzer().query(query);

        LogicalPlan plan = logicalOptimizer.optimize(analyzed);

        as(plan, Project.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        assertThat(plan.collect(Join.class), hasSize(blocks));
        assertThat(Expressions.names(plan.output()), equalTo(Expressions.names(analyzed.output())));
    }

    private <T extends LogicalPlan> T hoistedPast(String query, Class<T> nodeType) {
        return hoistedOnce(query, nodeType).collect(nodeType).getFirst();
    }

    private LogicalPlan hoistedOnce(String query, Class<? extends LogicalPlan> nodeType) {
        return hoistedOnce(substituted(query), nodeType);
    }

    private static LogicalPlan hoistedOnce(LogicalPlan before, Class<? extends LogicalPlan> nodeType) {
        assertTrue("the plan must have a Project directly below a " + nodeType.getSimpleName(), hasProjectChild(before, nodeType));

        LogicalPlan plan = new HoistProjects().apply(before);

        assertFalse(hasProjectChild(plan, nodeType));
        as(plan, Project.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        assertThat(plan.output(), equalTo(before.output()));
        return plan;
    }

    private static boolean hasProjectChild(LogicalPlan plan, Class<? extends LogicalPlan> nodeType) {
        return plan.anyMatch(p -> nodeType.isInstance(p) && p.children().stream().anyMatch(c -> c instanceof Project));
    }

    private static Completion completion(LogicalPlan child, Attribute prompt, String targetName) {
        return new Completion(
            EMPTY,
            child,
            Literal.keyword(EMPTY, "inference-id"),
            new Literal(EMPTY, 1000, DataType.INTEGER),
            prompt,
            referenceAttribute(targetName, DataType.KEYWORD)
        );
    }

    private static EsRelation relation() {
        return new EsRelation(
            EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(),
            List.<Attribute>of(getFieldAttribute("field", DataType.KEYWORD))
        );
    }

    private static String lookupBlocks(int count) {
        return lookupBlocks(count, "");
    }

    private static String lookupBlocks(int count, String extraCommandFormat) {
        StringBuilder query = new StringBuilder("FROM test");
        for (int i = 0; i < count; i++) {
            query.append("\n| EVAL language_code = languages + ")
                .append(i)
                .append("\n| LOOKUP JOIN languages_lookup ON language_code\n| RENAME language_name AS lang")
                .append(i)
                .append(String.format(Locale.ROOT, extraCommandFormat, i))
                .append("\n| DROP language_code");
        }
        return query.toString();
    }

    private LogicalPlan substituted(String query) {
        return new TestSubstitutionOnlyOptimizer(logicalOptimizerCtx).optimize(defaultAnalyzer().query(query));
    }

    private int operatorRunsToConverge(String query) {
        LogicalPlan analyzed = defaultAnalyzer().query(query);
        for (int runs = 1; runs <= 100; runs++) {
            try {
                new OperatorRunsLimitedOptimizer(logicalOptimizerCtx, runs).optimize(analyzed);
                return runs;
            } catch (RuleExecutionException e) {
                assertThat(e.getMessage(), containsString("Rule execution limit [" + runs + "] reached"));
            }
        }
        throw new AssertionError("operator batch did not converge in 100 runs");
    }

    private static final class OperatorRunsLimitedOptimizer extends LogicalPlanOptimizer {
        private final int maxRuns;

        OperatorRunsLimitedOptimizer(LogicalOptimizerContext context, int maxRuns) {
            super(context);
            this.maxRuns = maxRuns;
        }

        @Override
        protected List<RuleExecutor.Batch<LogicalPlan>> batches() {
            String operatorsName = operators().name();
            List<RuleExecutor.Batch<LogicalPlan>> batches = new ArrayList<>();
            for (RuleExecutor.Batch<LogicalPlan> batch : super.batches()) {
                batches.add(
                    batch.name().equals(operatorsName)
                        ? new RuleExecutor.Batch<>(batch.name(), new RuleExecutor.Limiter(maxRuns), batch.rules())
                        : batch
                );
            }
            return batches;
        }
    }
}
