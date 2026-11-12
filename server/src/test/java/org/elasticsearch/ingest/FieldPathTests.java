/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class FieldPathTests extends ESTestCase {

    /**
     * A template that renders to the value of the document's <code>name</code> field, in order to have something that is genuinely
     * different from one document to the next.
     */
    private static final TemplateScript.Factory NAME_TEMPLATE = params -> new TemplateScript(params) {
        @Override
        public String execute() {
            return (String) params.get("name");
        }
    };

    private static IngestDocument documentNamed(String name) {
        IngestDocument document = TestIngestDocument.emptyIngestDocument();
        document.setFieldValue("name", name);
        return document;
    }

    public void testConstantTemplateIsParsedOnlyOnce() {
        TemplateScript.Factory template = ConfigurationUtils.compileTemplate(
            "test",
            "tag",
            "field",
            "foo.bar",
            TestTemplateService.instance()
        );
        // this is what makes it safe to pre-parse the template
        assertThat(template, instanceOf(ConfigurationUtils.ConstantTemplateScriptFactory.class));

        FieldPath fieldPathTemplate = FieldPath.of(template);
        FieldPath first = fieldPathTemplate.resolve(documentNamed("one"));
        FieldPath second = fieldPathTemplate.resolve(documentNamed("two"));
        assertThat(first.toString(), equalTo("foo.bar"));
        assertThat(second, sameInstance(first));
    }

    public void testConstantPathIsParsedOnlyOnce() {
        FieldPath fieldPathTemplate = FieldPath.of("foo.bar");
        FieldPath first = fieldPathTemplate.resolve(documentNamed("one"));
        assertThat(first.toString(), equalTo("foo.bar"));
        assertThat(fieldPathTemplate.resolve(documentNamed("two")), sameInstance(first));
    }

    public void testRealTemplateIsRenderedForEveryDocument() {
        FieldPath fieldPathTemplate = FieldPath.of(NAME_TEMPLATE);
        FieldPath first = fieldPathTemplate.resolve(documentNamed("foo.bar"));
        FieldPath second = fieldPathTemplate.resolve(documentNamed("baz"));
        assertThat(first.toString(), equalTo("foo.bar"));
        assertThat(second.toString(), equalTo("baz"));
        assertThat(second, not(sameInstance(first)));
    }

    public void testRealTemplateWithMalformedResult() {
        FieldPath fieldPathTemplate = FieldPath.of(NAME_TEMPLATE);
        assertThat(fieldPathTemplate.resolve(documentNamed("fine")).toString(), equalTo("fine"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fieldPathTemplate.resolve(documentNamed("a..b")));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
        // a bad document does not poison later ones
        assertThat(fieldPathTemplate.resolve(documentNamed("fine")).toString(), equalTo("fine"));
    }

    /**
     * Pipelines are stored, so a pipeline that contains a malformed path has to still load. It has only ever failed when a document is
     * run through it, and that has to stay the case, so creating a {@link FieldPath} must not throw.
     */
    public void testMalformedConstantPathFailsOnlyWhenResolved() {
        TemplateScript.Factory template = ConfigurationUtils.compileTemplate(
            "test",
            "tag",
            "field",
            "a..b",
            TestTemplateService.instance()
        );
        assertThat(template, instanceOf(ConfigurationUtils.ConstantTemplateScriptFactory.class));

        for (FieldPath fieldPathTemplate : new FieldPath[] { FieldPath.of(template), FieldPath.of("a..b") }) {
            // and it fails the same way every time, not just the first time
            for (int i = 0; i < 3; i++) {
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> fieldPathTemplate.resolve(documentNamed("irrelevant"))
                );
                assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
            }
        }
    }

    /**
     * Required paths are never null, and if one is that is a programming error, which is better found when the processor is created than
     * when it is first used.
     */
    public void testNullRequiredPathIsRejected() {
        expectThrows(NullPointerException.class, () -> FieldPath.of((String) null));
    }

    /**
     * A path that is merely malformed or empty, on the other hand, is not rejected, as pipelines are stored, so those still wait for
     * {@link FieldPath#resolve(IngestDocument)}.
     */
    public void testRequiredPathThatIsMalformedOrEmptyIsNotRejected() {
        assertNotNull(FieldPath.of("a..b"));
        assertNotNull(FieldPath.of(""));
    }

    /**
     * Optional paths on the other hand (like a set processor's <code>copy_from</code>) are, when they are not there, null.
     */
    public void testNullOptionalPathIsNull() {
        assertNull(FieldPath.ofNullable(null));
        assertThat(FieldPath.ofNullable("foo.bar").toString(), equalTo("foo.bar"));
    }

    /**
     * Unlike a null path, which means that there isn't one, a path that is present but empty is a (malformed) path.
     */
    public void testEmptyConstantPathsFailOnlyWhenResolved() {
        for (FieldPath fieldPathTemplate : new FieldPath[] {
            FieldPath.of(""),
            FieldPath.of(ConfigurationUtils.compileTemplate("test", "tag", "field", "", TestTemplateService.instance())) }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> fieldPathTemplate.resolve(documentNamed("irrelevant"))
            );
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    /**
     * A null template is a programming error, and it is better for that to be found when the processor is created than for every document.
     */
    public void testTemplateThatRendersAsNullFailsOnlyWhenResolved() {
        FieldPath rendersAsNull = FieldPath.of(params -> new TemplateScript(params) {
            @Override
            public String execute() {
                return null;
            }
        });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> rendersAsNull.resolve(documentNamed("irrelevant")));
        assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
    }

    public void testNullTemplateIsRejected() {
        expectThrows(NullPointerException.class, () -> FieldPath.of((TemplateScript.Factory) null));
    }

    public void testToString() {
        assertThat(FieldPath.of("foo.bar").toString(), equalTo("foo.bar"));
        // malformed and empty paths are still shown the way that they were configured
        assertThat(FieldPath.of("a..b").toString(), equalTo("a..b"));
        assertThat(FieldPath.of("").toString(), equalTo(""));
        TemplateScript.Factory constantTemplate = ConfigurationUtils.compileTemplate(
            "test",
            "tag",
            "field",
            "foo.bar",
            TestTemplateService.instance()
        );
        assertThat(FieldPath.of(constantTemplate).toString(), equalTo("foo.bar"));
        assertThat(FieldPath.of(NAME_TEMPLATE).toString(), equalTo("[templated path]"));
    }

    /**
     * A path that is only invalid for some access patterns is fine to parse up front, the access pattern is checked when it is used.
     */
    public void testPathThatIsOnlyInvalidForSomeAccessPatternsCanBeCreated() {
        FieldPath fieldPathTemplate = FieldPath.of("a[0]");
        assertThat(fieldPathTemplate.resolve(documentNamed("irrelevant")).toString(), equalTo("a[0]"));
    }
}
