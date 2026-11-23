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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
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
        mappingBuilder.build(MergeReason.MAPPING_UPDATE);
        return mappingBuilder.toCompressedXContent().string();
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
     * The suppression of {@code include_in_root} happens when the mapping is built and only ever sets it to false, so
     * building again, which reads the already suppressed value of each ancestor, must keep every suppression of the first build.
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

        Exception e = expectThrows(Exception.class, () -> parse(duplicateInsideDissolvedObject).normalize(MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("cannot be changed from type [keyword] to [text]"));

        MappingBuilder normalizedAsTemplate = parse(duplicateInsideDissolvedObject);
        normalizedAsTemplate.normalize(MergeReason.INDEX_TEMPLATE);
        String template = normalizedAsTemplate.toCompressedXContent().string();
        assertThat(template, containsString("\"text\""));
        assertThat(template, not(containsString("keyword")));

        // serializing an unnormalized tree assumes MAPPING_UPDATE, so a template has to be normalized under its own reason first
        Exception unnormalized = expectThrows(Exception.class, () -> parse(duplicateInsideDissolvedObject).toCompressedXContent());
        assertThat(unnormalized.getCause().getMessage(), containsString("cannot be changed from type [keyword] to [text]"));
        MappingBuilder normalizedTemplate = parse(duplicateInsideDissolvedObject);
        normalizedTemplate.normalize(MergeReason.INDEX_TEMPLATE);
        assertThat(normalizedTemplate.toCompressedXContent().string(), equalTo(template));
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

        Exception e = expectThrows(Exception.class, () -> parse(nestedInsideDissolvedObject).normalize(MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("mapper [m.n.b.x] cannot be changed from type [keyword] to [text]"));
    }

    /**
     * Two declarations of the same nested object, one through object notation and one through a dotted key, are merged
     * before {@code include_in_root} is suppressed, so an explicit {@code include_in_root} that the merged object no longer
     * copies to its parent is kept.
     */
    public void testExplicitIncludeInRootSurvivesMergeOfDuplicateNestedDeclarations() throws IOException {
        String duplicateNested = """
            {
              "_doc": {
                "properties": {
                  "p": {
                    "properties": {
                      "n": {
                        "type": "nested", "include_in_parent": true, "include_in_root": true,
                        "properties": { "x": { "type": "keyword" } }
                      }
                    }
                  },
                  "p.n": { "type": "nested", "include_in_parent": false }
                }
              }
            }
            """;

        Mapping mapping = parse(duplicateNested).build(MergeReason.INDEX_TEMPLATE);

        NestedObjectMapper nested = (NestedObjectMapper) ((ObjectMapper) mapping.getRoot().getMapper("p")).getMapper("n");
        assertFalse(nested.isIncludeInParent());
        assertTrue(nested.isIncludeInRoot());
    }

    /**
     * Objects that are flattened away in a columnar index record their {@code dynamic} setting by prefix on the root. That
     * includes the settings of objects inside an object that is flattened away itself.
     */
    public void testPrefixPropertiesOfObjectsInsideAFlattenedObjectReachTheRoot() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        String nestedObjects = """
            {
              "_doc": {
                "properties": {
                  "foo": {
                    "dynamic": "false", "subobjects": false,
                    "properties": {
                      "bar": { "dynamic": "true", "properties": { "x": { "type": "keyword" } } }
                    }
                  }
                }
              }
            }
            """;

        RootObjectMapper root = mapperService.parseMappings(new CompressedXContent(nestedObjects))
            .build(MergeReason.MAPPING_UPDATE)
            .getRoot();

        assertThat(root.getPrefixProperties().get("foo").dynamic(), equalTo(ObjectMapper.Dynamic.FALSE));
        assertThat(root.getPrefixProperties().get("foo.bar").dynamic(), equalTo(ObjectMapper.Dynamic.TRUE));
    }

    /**
     * When duplicate declarations of a nested object merge, what the nested objects inside it are told about their parent has to
     * be what the merged object says, not what the first declaration said.
     */
    public void testNestedObjectsSeeTheMergedIncludeInRootOfTheirParent() throws IOException {
        String duplicateParents = """
            {
              "_doc": {
                "properties": {
                  "p": {
                    "properties": {
                      "n": {
                        "type": "nested", "include_in_root": true,
                        "properties": { "c": { "type": "nested", "include_in_parent": true, "include_in_root": true } }
                      }
                    }
                  },
                  "p.n": { "type": "nested", "include_in_root": false }
                }
              }
            }
            """;

        Mapping mapping = parse(duplicateParents).build(MergeReason.INDEX_TEMPLATE);

        NestedObjectMapper parent = (NestedObjectMapper) ((ObjectMapper) mapping.getRoot().getMapper("p")).getMapper("n");
        assertFalse(parent.isIncludeInRoot());
        NestedObjectMapper child = (NestedObjectMapper) parent.getMapper("c");
        assertTrue(child.isIncludeInRoot());
    }

    /**
     * A disabled object drops everything below it, so the settings of the objects it held are not recorded by prefix: a longer
     * prefix inside the disabled one would let a field there be mapped dynamically.
     */
    public void testPrefixPropertiesBelowADisabledObjectAreNotRecorded() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        String disabledObject = """
            {
              "_doc": {
                "properties": {
                  "foo": {
                    "enabled": false, "subobjects": false,
                    "properties": {
                      "bar": { "dynamic": "true", "properties": { "x": { "type": "keyword" } } }
                    }
                  }
                }
              }
            }
            """;

        RootObjectMapper root = mapperService.parseMappings(new CompressedXContent(disabledObject))
            .build(MergeReason.MAPPING_UPDATE)
            .getRoot();

        assertThat(root.getPrefixProperties().get("foo").enabled(), equalTo(false));
        assertNull(root.getPrefixProperties().get("foo.bar"));
    }

    /**
     * An object declared twice, through object notation and through a dotted key, is merged before what is below it is normalized,
     * so that what is below sees the settings the merged object carries and not those of one of the declarations.
     */
    public void testDuplicateParentsAreMergedBeforeTheirDescendantsAreNormalized() throws IOException {
        String duplicateParents = """
            {
              "_doc": {
                "properties": {
                  "a": { "dynamic": "false", "properties": { "x": { "type": "keyword" } } },
                  "a.b": {
                    "type": "object", "subobjects": false,
                    "properties": { "c": { "type": "object", "dynamic": "false", "properties": { "y": { "type": "keyword" } } } }
                  }
                }
              }
            }
            """;

        Mapping mapping = parse(duplicateParents).build(MergeReason.MAPPING_UPDATE);

        assertNotNull(mapping.getRoot().getMapper("a"));
    }

    private static final String HOISTED_FIELD_THEN_DIRECT_FIELD = """
        {
          "_doc": {
            "subobjects": false,
            "properties": {
              "a": { "properties": { "x": { "type": "keyword", "ignore_above": 10 } } },
              "a.x": { "type": "text" }
            }
          }
        }
        """;

    /**
     * Under {@code subobjects: false} a field inside a plain object is hoisted to a dotted name, and a later declaration of that
     * dotted name is merged with it by the rules of the merge reason, which here reject the change of type.
     */
    public void testFieldDeclaredAgainAfterBeingHoistedIsMergedAsAConflict() throws IOException {
        Exception e = expectThrows(Exception.class, () -> parse(HOISTED_FIELD_THEN_DIRECT_FIELD).build(MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("cannot be changed from type [keyword] to [text]"));
    }

    public void testFieldDeclaredAgainAfterBeingHoistedTakesTheLaterDeclarationInATemplate() throws IOException {
        Mapping mapping = parse(HOISTED_FIELD_THEN_DIRECT_FIELD).build(MergeReason.INDEX_TEMPLATE);

        assertThat(mapping.getRoot().getMapper("a.x"), instanceOf(TextFieldMapper.class));
    }
}
