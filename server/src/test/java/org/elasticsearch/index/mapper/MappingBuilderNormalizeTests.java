/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * {@link Mapper.Builder#normalize} must leave a builder tree holding the same children the built mappers hold, and must
 * be idempotent, because {@code build} normalizes first and serialization normalizes again.
 */
public class MappingBuilderNormalizeTests extends MapperServiceTestCase {

    /** The same field declared through object notation and through dot notation, as in {@code MappingParserTests}. */
    private static final String OBJECT_AND_DOT_NOTATION = """
        {
          "_doc": {
            "properties": {
              "obj": {
                "properties": {
                  "source": { "properties": { "geo": { "properties": { "location": { "type": "geo_point" } } } } }
                }
              },
              "obj.source.geo.location": { "type": "geo_point" }
            }
          }
        }
        """;

    /** A plain object under {@code subobjects: false}, which is flattened to a dotted leaf. */
    private static final String FLATTENED_OBJECT = """
        {
          "_doc": {
            "subobjects": false,
            "properties": {
              "a": { "properties": { "x": { "type": "keyword" } } },
              "a.y": { "type": "keyword" }
            }
          }
        }
        """;

    /**
     * Three levels of nested objects, each with {@code include_in_parent} and {@code include_in_root} both set, so that
     * the suppression of {@code include_in_root} applies at every level below the first.
     */
    private static final String NESTED_IN_NESTED = """
        {
          "_doc": {
            "properties": {
              "a": {
                "type": "nested", "include_in_parent": true,
                "properties": {
                  "b": {
                    "type": "nested", "include_in_root": true, "include_in_parent": true,
                    "properties": {
                      "c": {
                        "type": "nested", "include_in_root": true, "include_in_parent": true,
                        "properties": { "f": { "type": "keyword" } }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """;

    private static final String[] SHAPES = { OBJECT_AND_DOT_NOTATION, FLATTENED_OBJECT, NESTED_IN_NESTED };

    private MappingBuilder parse(String mapping) throws IOException {
        return createMapperService(mapping(b -> {})).parseMappings(new CompressedXContent(mapping));
    }

    private static List<Mapper.Builder> children(Mapper.Builder builder) {
        assertThat(builder, instanceOf(ObjectMapper.Builder.class));
        return ((ObjectMapper.Builder) builder).getChildBuilders();
    }

    private static String built(MappingBuilder mappingBuilder) {
        return mappingBuilder.build(MergeReason.MAPPING_UPDATE).toCompressedXContent().string();
    }

    public void testNormalizeMergesDuplicateChildren() throws IOException {
        MappingBuilder mappingBuilder = parse(OBJECT_AND_DOT_NOTATION);
        // parsing keeps both declarations of "obj": the object-notation one and the intermediate built from the dotted key
        assertThat(children(mappingBuilder.rootBuilder()), hasSize(2));

        mappingBuilder.normalize(MergeReason.MAPPING_UPDATE);

        Mapper.Builder current = mappingBuilder.rootBuilder();
        for (String name : List.of("obj", "source", "geo")) {
            List<Mapper.Builder> children = children(current);
            assertThat("children of " + current.leafName(), children, hasSize(1));
            current = children.get(0);
            assertThat(current.leafName(), equalTo(name));
        }
        List<Mapper.Builder> leaves = children(current);
        assertThat(leaves, hasSize(1));
        assertThat(leaves.get(0), instanceOf(FieldMapper.Builder.class));
        assertThat(leaves.get(0).leafName(), equalTo("location"));
    }

    public void testNormalizeFlattensObjectsUnderDisabledSubobjects() throws IOException {
        MappingBuilder mappingBuilder = parse(FLATTENED_OBJECT);
        assertThat(children(mappingBuilder.rootBuilder()), hasSize(2));

        mappingBuilder.normalize(MergeReason.MAPPING_UPDATE);

        List<Mapper.Builder> children = children(mappingBuilder.rootBuilder());
        assertThat(children, hasSize(2));
        for (Mapper.Builder child : children) {
            assertThat("no plain object builder is left to flatten", child, instanceOf(FieldMapper.Builder.class));
        }
        assertThat(new HashSet<>(children.stream().map(Mapper.Builder::leafName).toList()), equalTo(new HashSet<>(List.of("a.x", "a.y"))));
    }

    public void testNormalizeTwiceChangesNothing() throws IOException {
        for (String shape : SHAPES) {
            MappingBuilder mappingBuilder = parse(shape);
            mappingBuilder.normalize(MergeReason.MAPPING_UPDATE);
            List<Mapper.Builder> once = List.copyOf(children(mappingBuilder.rootBuilder()));

            mappingBuilder.normalize(MergeReason.MAPPING_UPDATE);

            assertThat(shape, new HashSet<>(children(mappingBuilder.rootBuilder())), equalTo(new HashSet<>(once)));
        }
    }

    /**
     * Before {@code normalize} wrote its result back, a second {@code build} over a {@code subobjects: false} tree
     * flattened the already-renamed leaves again, so {@code a.x} became {@code a.a.x}.
     */
    public void testBuildTwiceGivesTheSameMapping() throws IOException {
        for (String shape : SHAPES) {
            MappingBuilder mappingBuilder = parse(shape);
            assertThat(shape, built(mappingBuilder), equalTo(built(mappingBuilder)));
        }
    }

    public void testNormalizingFirstDoesNotChangeTheBuiltMapping() throws IOException {
        for (String shape : SHAPES) {
            MappingBuilder normalized = parse(shape);
            normalized.normalize(MergeReason.MAPPING_UPDATE);
            assertThat(shape, built(normalized), equalTo(built(parse(shape))));
        }
    }

    public void testBuildingAfterNormalizingTwiceDoesNotChangeTheBuiltMapping() throws IOException {
        for (String shape : SHAPES) {
            MappingBuilder normalized = parse(shape);
            normalized.normalize(MergeReason.MAPPING_UPDATE);
            normalized.normalize(MergeReason.MAPPING_UPDATE);
            assertThat(shape, built(normalized), equalTo(built(parse(shape))));
        }
    }

    /**
     * The suppression of {@code include_in_root} only ever sets it to false, so a second pass, which reads the already
     * suppressed value of each ancestor, must keep every suppression the first pass applied.
     */
    public void testIncludeInRootSuppressionSurvivesRepeatedNormalization() throws IOException {
        String once = built(parse(NESTED_IN_NESTED));
        assertThat(once, containsString("include_in_parent"));
        assertThat(once, not(containsString("include_in_root")));

        MappingBuilder repeated = parse(NESTED_IN_NESTED);
        repeated.normalize(MergeReason.MAPPING_UPDATE);
        repeated.normalize(MergeReason.MAPPING_UPDATE);
        String afterRepeated = built(repeated);
        assertThat(afterRepeated, equalTo(once));
        assertThat(built(repeated), equalTo(once));
    }

    /**
     * What a {@code subobjects: false} parent hoists out of a plain child must already be merged. Normalizing the child
     * first applies the merge reason's rules to a field it declares twice; hoisting the two declarations unmerged would
     * let the later one silently replace the earlier whatever the reason. Templates keep their last-wins behaviour, so
     * a template that works today must not start to throw.
     */
    public void testDuplicatesInsideADissolvedObjectAreMergedBeforeHoisting() throws IOException {
        String duplicateInsideDissolvedObject = """
            {
              "_doc": {
                "subobjects": false,
                "properties": {
                  "a": {
                    "properties": {
                      "b": { "properties": { "x": { "type": "keyword" } } },
                      "b.x": { "type": "text" }
                    }
                  }
                }
              }
            }
            """;

        Exception e = expectThrows(Exception.class, () -> built(parse(duplicateInsideDissolvedObject)));
        assertThat(e.getMessage(), containsString("cannot be changed from type [keyword] to [text]"));

        String template = parse(duplicateInsideDissolvedObject).build(MergeReason.INDEX_TEMPLATE).toCompressedXContent().string();
        assertThat(template, containsString("\"text\""));
        assertThat(template, not(containsString("keyword")));
    }

    /**
     * A nested object inside a plain object that a {@code subobjects: false} parent dissolves is renamed to its dotted
     * path when it is hoisted. It has already been normalized by then, so an error from inside it has to name the same
     * full path the renamed builder would have produced.
     */
    public void testErrorInsideNestedObjectHoistedOutOfADissolvedObjectNamesItsFullPath() throws IOException {
        String nestedInsideDissolvedObject = """
            {
              "_doc": {
                "subobjects": false,
                "properties": {
                  "m": {
                    "properties": {
                      "n": {
                        "type": "nested",
                        "properties": {
                          "b": { "properties": { "x": { "type": "keyword" } } },
                          "b.x": { "type": "text" }
                        }
                      }
                    }
                  }
                }
              }
            }
            """;

        Exception e = expectThrows(Exception.class, () -> built(parse(nestedInsideDissolvedObject)));
        assertThat(e.getMessage(), containsString("mapper [m.n.b.x] cannot be changed from type [keyword] to [text]"));
    }
}
