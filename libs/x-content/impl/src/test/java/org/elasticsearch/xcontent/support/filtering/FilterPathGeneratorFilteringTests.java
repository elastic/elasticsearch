/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.support.filtering;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.filter.FilteringGeneratorDelegate;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.provider.filtering.FilterPathBasedFilter;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class FilterPathGeneratorFilteringTests extends ESTestCase {

    private final JsonFactory JSON_FACTORY = new JsonFactory();

    public void testInclusiveFilters() throws Exception {
        final String SAMPLE = """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""";

        assertResult(SAMPLE, "a", true, """
            {
              "a": 0
            }""");
        assertResult(SAMPLE, "b", true, """
            {
              "b": true
            }""");
        assertResult(SAMPLE, "c", true, """
            {
              "c": "c_value"
            }""");
        assertResult(SAMPLE, "d", true, """
            {
              "d": [ 0, 1, 2 ]
            }""");
        assertResult(SAMPLE, "e", true, """
            {
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "z", true, "");
        assertResult(SAMPLE, "e.f1", true, """
            {
              "e": [ { "f1": "f1_value" } ]
            }""");
        assertResult(SAMPLE, "e.f2", true, """
            {
               "e": [ { "f2": "f2_value" } ]
            }""");
        assertResult(SAMPLE, "e.f*", true, """
            {
              "e": [ { "f1": "f1_value", "f2": "f2_value" } ]
            }""");
        assertResult(SAMPLE, "e.*2", true, """
            {
              "e": [ { "f2": "f2_value" }, { "g2": "g2_value" } ]
            }""");

        assertResult(SAMPLE, "h.i", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.i.j", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.i.j.k", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.i.j.k.l", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");

        assertResult(SAMPLE, "h.*", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "*.i", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");

        assertResult(SAMPLE, "*.i.j", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.*.j", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.i.*", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");

        assertResult(SAMPLE, "*.i.j.k", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.*.j.k", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.i.*.k", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.i.j.*", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");

        assertResult(SAMPLE, "*.i.j.k.l", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.*.j.k.l", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.i.*.k.l", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.i.j.*.l", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h.i.j.k.*", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");

        assertResult(SAMPLE, "h.*.j.*.l", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "**.l", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");

        assertResult(SAMPLE, "**.*2", true, """
            {
              "e": [ { "f2": "f2_value" }, { "g2": "g2_value" } ]
            }""");

        assertResult(SAMPLE, "h.i.j.k.l,h.i.j.k.l.m", true, """
            {
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "a,b,c,d,e.f1,e.f2,e.g1,e.g2,h.i.j.k.l", true, SAMPLE);
        assertResult(SAMPLE, "", true, "");
        assertResult(SAMPLE, "h.", true, "");
    }

    public void testExclusiveFilters() throws Exception {
        final String SAMPLE = """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""";

        assertResult(SAMPLE, "a", false, """
            {
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "b", false, """
            {
              "a": 0,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "c", false, """
            {
              "a": 0,
              "b": true,
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "d", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "e", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "h", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "z", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "e.f1", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "e.f2", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value" }, { "g1": "g1_value", "g2": "g2_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "e.f*", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "g1": "g1_value", "g2": "g2_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");
        assertResult(SAMPLE, "e.*2", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value" }, { "g1": "g1_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");

        assertResult(SAMPLE, "h.i", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.i.j", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.i.j.k", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.i.j.k.l", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");

        assertResult(SAMPLE, "h.*", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "*.i", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");

        assertResult(SAMPLE, "*.i.j", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.*.j", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.i.*", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");

        assertResult(SAMPLE, "*.i.j.k", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.*.j.k", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.i.*.k", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.i.j.*", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");

        assertResult(SAMPLE, "*.i.j.k.l", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.*.j.k.l", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.i.*.k.l", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.i.j.*.l", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "h.i.j.k.*", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");

        assertResult(SAMPLE, "h.*.j.*.l", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");
        assertResult(SAMPLE, "**.l", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");

        assertResult(SAMPLE, "**.*2", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value" }, { "g1": "g1_value" } ],
              "h": {
                "i": {
                  "j": {
                    "k": {
                      "l": "l_value"
                    }
                  }
                }
              }
            }""");

        assertResult(SAMPLE, "h.i.j.k.l,h.i.j.k.l.m", false, """
            {
              "a": 0,
              "b": true,
              "c": "c_value",
              "d": [ 0, 1, 2 ],
              "e": [ { "f1": "f1_value", "f2": "f2_value" }, { "g1": "g1_value", "g2": "g2_value" } ]
            }""");

        assertResult(SAMPLE, "a,b,c,d,e.f1,e.f2,e.g1,e.g2,h.i.j.k.l", false, "");
        assertResult(SAMPLE, "", false, SAMPLE);
        assertResult(SAMPLE, "h.", false, SAMPLE);
    }

    public void testInclusiveFiltersWithDots() throws Exception {
        assertResult("""
            {
              "a": 0,
              "b.c": "value",
              "b": {
                "c": "c_value"
              }
            }""", "b.c", true, """
            {
              "b": {
                "c": "c_value"
              }
            }""");
        assertResult("""
            {
              "a": 0,
              "b.c": "value",
              "b": {
                "c": "c_value"
              }
            }""", "b\\.c", true, """
            {
              "b.c": "value"
            }""");
    }

    public void testExclusiveFiltersWithDots() throws Exception {
        assertResult("""
            {
              "a": 0,
              "b.c": "value",
              "b": {
                "c": "c_value"
              }
            }""", "b.c", false, """
            {
              "a": 0,
              "b.c": "value"
            }""");
        assertResult("""
            {
              "a": 0,
              "b.c": "value",
              "b": {
                "c": "c_value"
              }
            }""", "b\\.c", false, """
            {
              "a": 0,
              "b": {
                "c": "c_value"
              }
            }""");
    }

    private void assertResult(String input, String filter, boolean inclusive, String expected) throws Exception {
        try (BytesStreamOutput os = new BytesStreamOutput()) {
            try (
                FilteringGeneratorDelegate generator = new FilteringGeneratorDelegate(
                    JSON_FACTORY.createGenerator(os),
                    new FilterPathBasedFilter(Arrays.asList(filter.split(",")).stream().collect(Collectors.toSet()), inclusive),
                    true,
                    true
                )
            ) {
                try (JsonParser parser = JSON_FACTORY.createParser(input)) {
                    while (parser.nextToken() != null) {
                        generator.copyCurrentStructure(parser);
                    }
                }
            }
            assertThat(os.bytes().utf8ToString().replaceAll("\\s", ""), equalTo(expected.replaceAll("\\s", "")));
        }
    }
}
