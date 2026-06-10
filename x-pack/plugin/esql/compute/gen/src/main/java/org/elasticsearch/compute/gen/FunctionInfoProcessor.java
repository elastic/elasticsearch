/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.processing.Completion;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

/**
 * Finds classes with {@code FunctionInfo} annotated constructors and extracts
 * their class Javadoc into use facing descriptions.
 * <ul>
 *     <li>
 *         Write a short description in the first sentence. The following paragraphs
 *         are all converted into a top level description used in all user facing docs.
 *     </li>
 *     <li>
 *         Everything except {@code {@link }} is converted to Markdown with fairly gnarly regexes.
 *         {@code {@link }} tags are stored as-is and expanded to Markdown links at render time
 *         by {@code DocsV3Support}. We'll convert the remaining regexes to Markdown Javadoc
 *         once we're on Java 25 and they bother someone.
 *     </li>
 *     <li>
 *         Non-ascii characters aren't allowed, mostly as a way to exclude curly apostrophes.
 *         If we find a use for non-ascii characters, we should make a pass list.
 *     </li>
 *     <li>
 *         Docs inside {@code <h2>Implementation</h2>} or {@code <h2>Extra Notes</h2>}
 *         are not shown to the user. Use {@code <h2>Implementation</h2>} for internal
 *         notes. Use {@code <h2>Extra Notes</h2>} for good user-facing documentation
 *         that we'd like to surface in the rendered docs but don't yet have a mechanism
 *         for including.
 *     </li>
 * </ul>
 */
public class FunctionInfoProcessor implements Processor {
    private static final String FUNCTION_INFO = "org.elasticsearch.xpack.esql.expression.function.FunctionInfo";
    private static final String PARAM = "org.elasticsearch.xpack.esql.expression.function.Param";
    private static final String MAP_PARAM = "org.elasticsearch.xpack.esql.expression.function.MapParam";
    private static final String EXAMPLE = "org.elasticsearch.xpack.esql.expression.function.Example";
    private static final String EXAMPLES = "org.elasticsearch.xpack.esql.expression.function.Examples";

    private ProcessingEnvironment env;

    @Override
    public Set<String> getSupportedOptions() {
        return Set.of();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(FUNCTION_INFO, PARAM, MAP_PARAM, EXAMPLE, EXAMPLES);
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_21;
    }

    @Override
    public void init(ProcessingEnvironment processingEnvironment) {
        this.env = processingEnvironment;
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (TypeElement annotation : annotations) {
            if (annotation.getQualifiedName().toString().equals(FUNCTION_INFO) == false) {
                continue;
            }
            for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
                if (element instanceof ExecutableElement constructor) {
                    processConstructor(constructor);
                }
            }
        }
        return true;
    }

    private void processConstructor(ExecutableElement constructor) {
        TypeElement enclosingClass = (TypeElement) constructor.getEnclosingElement();
        String qualifiedName = enclosingClass.getQualifiedName().toString();
        String resourcePath = "META-INF/esql/functions/" + qualifiedName.replace('.', '/') + ".json";
        String classJavadoc = env.getElementUtils().getDocComment(enclosingClass);

        if (classJavadoc == null || classJavadoc.isBlank()) {
            env.getMessager()
                .printMessage(
                    Diagnostic.Kind.ERROR,
                    "Function class " + qualifiedName + " is missing a class-level Javadoc description",
                    enclosingClass
                );
            return;
        }
        String markdown = toMarkdown(classJavadoc).strip();
        if (markdown.isEmpty()) {
            env.getMessager()
                .printMessage(
                    Diagnostic.Kind.ERROR,
                    "Function class " + qualifiedName + " has an empty Javadoc description after processing",
                    enclosingClass
                );
            return;
        }
        try {
            checkForDisallowedHtml(markdown);
            checkForNonAscii(markdown);
            Filer filer = env.getFiler();
            FileObject resource = filer.createResource(StandardLocation.CLASS_OUTPUT, "", resourcePath, constructor, enclosingClass);
            writeJson(resource, markdown);
        } catch (IllegalArgumentException e) {
            env.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage(), constructor);
        } catch (IOException e) {
            env.getMessager()
                .printMessage(
                    Diagnostic.Kind.ERROR,
                    "Failed to write function info for " + qualifiedName + ": " + e.getMessage(),
                    constructor
                );
        }
    }

    /**
     * We don't allow non-ascii characters mostly to catch the sneaky curly
     * apostrophe and em dash. AI loves to add them, but they are just in the way.
     * If you want non-ascii characters in the rendered output and are sure they
     * are ok, make an allowlist here.
     */
    /**
     * Unicode math symbols explicitly allowed in user-facing descriptions.
     * We allow these because they appear in mathematical definitions (e.g. ST_INTERSECTS).
     */
    private static final Set<Character> ALLOWED_NON_ASCII = Set.of(
        '\u21D4', // ⇔ DOUBLE LEFT RIGHT ARROW (iff)
        '\u22C2', // ⋂ N-ARY INTERSECTION
        '\u2205', // ∅ EMPTY SET
        '\u2260'  // ≠ NOT EQUAL TO
    );

    private static void checkForNonAscii(String markdown) {
        StringBuilder found = new StringBuilder();
        for (int i = 0; i < markdown.length(); i++) {
            char c = markdown.charAt(i);
            if (c > 127 && ALLOWED_NON_ASCII.contains(c) == false) {
                if (found.isEmpty() == false) {
                    found.append(", ");
                }
                found.append(String.format(Locale.ROOT, "U+%04X '%c'", (int) c, c));
            }
        }
        if (found.isEmpty() == false) {
            throw new IllegalArgumentException("illegal character in user facing description: " + found);
        }
    }

    /**
     * All HTML is translated into Markdown before it arrives here.
     */
    private static void checkForDisallowedHtml(String markdown) {
        Matcher m = HTML_TAG.matcher(markdown);
        StringBuilder found = new StringBuilder();
        while (m.find()) {
            if (found.isEmpty() == false) {
                found.append(", ");
            }
            found.append(m.group(0));
        }
        if (found.isEmpty() == false) {
            throw new IllegalArgumentException(
                "disallowed HTML tags in user facing description (use <a href=\"...\"> links or {@code} instead): " + found
            );
        }
    }

    private void writeJson(FileObject resource, String markdown) throws IOException {
        try (XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, resource.openOutputStream()).prettyPrint().lfAtEnd()) {
            b.startObject();
            b.field("description", markdown);
            b.endObject();
        }
    }

    private static final Pattern IMPL_SECTION = Pattern.compile("\\n\\s*<h2>(?:Implementation|Extra Notes)</h2>");
    private static final Pattern HTML_LINK = Pattern.compile("<a href=\"([^\"]+)\">([^<]+)</a>");
    private static final Pattern HTML_PARA = Pattern.compile("</?(?:p|ul|ol)>");
    private static final Pattern HTML_LI_OPEN = Pattern.compile("<li>");
    private static final Pattern HTML_LI_CLOSE = Pattern.compile("</li>");
    private static final Pattern HTML_STRONG = Pattern.compile("</?strong>");
    private static final Pattern HTML_TAG = Pattern.compile("</?[a-zA-Z][^>]*>");
    private static final Pattern JAVADOC_CODE = Pattern.compile("\\{@code\\s+([^}]+)}");
    private static final Pattern LEADING_WHITESPACE_PER_LINE = Pattern.compile("(?m)^ +");

    /**
     * Converts the Javadoc comment to markdown format for storage.
     * {@code {@link }} tags are left as-is for later expansion by {@code DocsV3Support}.
     * <ul>
     *   <li>Strips everything from {@code <h2>Implementation</h2>} or {@code <h2>Extra Notes</h2>} onward.</li>
     *   <li>Strips leading whitespace from each line (the Java compiler's {@code getDocComment} strips
     *       the leading {@code *} but leaves the space before content, e.g. {@code " * text"} becomes
     *       {@code " text"}).</li>
     *   <li>Converts {@code <a href="url">text</a>} to {@code [text](url)}.</li>
     *   <li>Converts {@code <p>}, {@code <ul>}, {@code <ol>} to newlines.</li>
     *   <li>Converts {@code <li>} to {@code - } list items.</li>
     *   <li>Converts {@code <strong>} to {@code **}.</li>
     *   <li>Converts {@code {@code text}} to {@code `text`}.</li>
     * </ul>
     */
    private static String toMarkdown(String javadoc) {
        Matcher implMatcher = IMPL_SECTION.matcher(javadoc);
        if (implMatcher.find()) {
            javadoc = javadoc.substring(0, implMatcher.start());
        }
        javadoc = LEADING_WHITESPACE_PER_LINE.matcher(javadoc).replaceAll("");
        javadoc = HTML_LINK.matcher(javadoc).replaceAll(r -> "[" + r.group(2) + "](" + r.group(1) + ")");
        javadoc = HTML_PARA.matcher(javadoc).replaceAll("\n\n");
        javadoc = HTML_LI_OPEN.matcher(javadoc).replaceAll("- ");
        javadoc = HTML_LI_CLOSE.matcher(javadoc).replaceAll("\n");
        javadoc = HTML_STRONG.matcher(javadoc).replaceAll("**");
        javadoc = JAVADOC_CODE.matcher(javadoc).replaceAll(r -> "`" + r.group(1) + "`");
        return javadoc;
    }

    @Override
    public Iterable<? extends Completion> getCompletions(
        Element element,
        AnnotationMirror annotationMirror,
        ExecutableElement executableElement,
        String s
    ) {
        return List.of();
    }
}
