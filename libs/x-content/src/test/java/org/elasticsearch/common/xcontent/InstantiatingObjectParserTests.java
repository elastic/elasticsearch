/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class InstantiatingObjectParserTests extends ESTestCase {

    public static class NoAnnotations {
        final int a;
        final String b;
        final long c;

        public NoAnnotations() {
            this(1, "2", 3);
        }

        private NoAnnotations(int a) {
            this(a, "2", 3);
        }

        public NoAnnotations(int a, String b) {
            this(a, b, 3);
        }

        public NoAnnotations(int a, long c) {
            this(a, "2", c);
        }

        public NoAnnotations(int a, String b, long c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NoAnnotations that = (NoAnnotations) o;
            return a == that.a &&
                c == that.c &&
                Objects.equals(b, that.b);
        }

        @Override
        public int hashCode() {
            return Objects.hash(a, b, c);
        }
    }

    public void testNoAnnotation() throws IOException {
        InstantiatingObjectParser.Builder<NoAnnotations, Void> builder = InstantiatingObjectParser.builder("foo", NoAnnotations.class);
        builder.declareInt(constructorArg(), new ParseField("a"));
        builder.declareString(constructorArg(), new ParseField("b"));
        builder.declareLong(constructorArg(), new ParseField("c"));
        InstantiatingObjectParser<NoAnnotations, Void> parser = builder.build();
        try (XContentParser contentParser = createParser(JsonXContent.jsonXContent, "{\"a\": 5, \"b\":\"6\", \"c\": 7 }")) {
            assertThat(parser.parse(contentParser, null), equalTo(new NoAnnotations(5, "6", 7)));
        }
    }

    public void testNoAnnotationWrongArgumentNumber() {
        InstantiatingObjectParser.Builder<NoAnnotations, Void> builder = InstantiatingObjectParser.builder("foo", NoAnnotations.class);
        builder.declareInt(constructorArg(), new ParseField("a"));
        builder.declareString(constructorArg(), new ParseField("b"));
        builder.declareLong(constructorArg(), new ParseField("c"));
        builder.declareLong(constructorArg(), new ParseField("d"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertThat(e.getMessage(), containsString("No public constructors with 4 parameters exist in the class"));
    }

    public void testAmbiguousConstructor() {
        InstantiatingObjectParser.Builder<NoAnnotations, Void> builder = InstantiatingObjectParser.builder("foo", NoAnnotations.class);
        builder.declareInt(constructorArg(), new ParseField("a"));
        builder.declareString(constructorArg(), new ParseField("b"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertThat(e.getMessage(), containsString(
            "More then one public constructor with 2 arguments found. The use of @ParserConstructor annotation is required"
        ));
    }

    public void testPrivateConstructor() {
        InstantiatingObjectParser.Builder<NoAnnotations, Void> builder = InstantiatingObjectParser.builder("foo", NoAnnotations.class);
        builder.declareInt(constructorArg(), new ParseField("a"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertThat(e.getMessage(), containsString("No public constructors with 1 parameters exist in the class "));
    }

    public static class LonelyArgument {
        public final int a;

        private String b;

        public LonelyArgument(int a) {
            this.a = a;
            this.b = "Not set";
        }

        public void setB(String b) {
            this.b = b;
        }

        public String getB() {
            return b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LonelyArgument that = (LonelyArgument) o;
            return a == that.a &&
                Objects.equals(b, that.b);
        }

        @Override
        public int hashCode() {
            return Objects.hash(a, b);
        }
    }

    public void testOneArgConstructor() throws IOException {
        InstantiatingObjectParser.Builder<LonelyArgument, Void> builder = InstantiatingObjectParser.builder("foo", LonelyArgument.class);
        builder.declareInt(constructorArg(), new ParseField("a"));
        InstantiatingObjectParser<LonelyArgument, Void> parser = builder.build();
        try (XContentParser contentParser = createParser(JsonXContent.jsonXContent, "{\"a\": 5 }")) {
            assertThat(parser.parse(contentParser, null), equalTo(new LonelyArgument(5)));
        }
    }

    public void testSetNonConstructor() throws IOException {
        InstantiatingObjectParser.Builder<LonelyArgument, Void> builder = InstantiatingObjectParser.builder("foo", LonelyArgument.class);
        builder.declareInt(constructorArg(), new ParseField("a"));
        builder.declareString(LonelyArgument::setB, new ParseField("b"));
        InstantiatingObjectParser<LonelyArgument, Void> parser = builder.build();
        try (XContentParser contentParser = createParser(JsonXContent.jsonXContent, "{\"a\": 5, \"b\": \"set\" }")) {
            LonelyArgument expected = parser.parse(contentParser, null);
            assertThat(expected.a, equalTo(5));
            assertThat(expected.b, equalTo("set"));
        }
    }

    public static class Annotations {
        final int a;
        final String b;
        final long c;

        public Annotations() {
            this(1, "2", 3);
        }

        public Annotations(int a, String b) {
            this(a, b, -1);
        }

        public Annotations(int a, String b, long c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        @ParserConstructor
        public Annotations(int a, String b, String c) {
            this.a = a;
            this.b = b;
            this.c = Long.parseLong(c);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Annotations that = (Annotations) o;
            return a == that.a &&
                c == that.c &&
                Objects.equals(b, that.b);
        }

        @Override
        public int hashCode() {
            return Objects.hash(a, b, c);
        }
    }

    public void testAnnotation() throws IOException {
        InstantiatingObjectParser.Builder<Annotations, Void> builder = InstantiatingObjectParser.builder("foo", Annotations.class);
        builder.declareInt(constructorArg(), new ParseField("a"));
        builder.declareString(constructorArg(), new ParseField("b"));
        builder.declareString(constructorArg(), new ParseField("c"));
        InstantiatingObjectParser<Annotations, Void> parser = builder.build();
        try (XContentParser contentParser = createParser(JsonXContent.jsonXContent, "{\"a\": 5, \"b\":\"6\", \"c\": \"7\"}")) {
            assertThat(parser.parse(contentParser, null), equalTo(new Annotations(5, "6", 7)));
        }
    }

    public void testAnnotationWrongArgumentNumber() {
        InstantiatingObjectParser.Builder<Annotations, Void> builder = InstantiatingObjectParser.builder("foo", Annotations.class);
        builder.declareInt(constructorArg(), new ParseField("a"));
        builder.declareString(constructorArg(), new ParseField("b"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertThat(e.getMessage(), containsString("Annotated constructor doesn't have 2 arguments in the class"));
    }

    public void testDoubleDeclarationThrowsException() throws IOException {
        class DoubleFieldDeclaration {
            private int intField;

            DoubleFieldDeclaration(int intField) {
                this.intField = intField;
            }
        }

        InstantiatingObjectParser.Builder<DoubleFieldDeclaration, Void> builder =
            InstantiatingObjectParser.builder("double_declaration", DoubleFieldDeclaration.class);
        builder.declareInt(constructorArg(), new ParseField("name"));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> builder.declareInt(constructorArg(), new ParseField("name")));

        assertThat(exception, instanceOf(IllegalArgumentException.class));
        assertThat(exception.getMessage(), startsWith("Parser already registered for name=[name]"));
    }
}
