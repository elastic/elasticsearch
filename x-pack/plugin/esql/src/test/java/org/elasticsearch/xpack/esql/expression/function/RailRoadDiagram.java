/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import net.nextencia.rrdiagram.grammar.model.Expression;
import net.nextencia.rrdiagram.grammar.model.GrammarToRRDiagram;
import net.nextencia.rrdiagram.grammar.model.Literal;
import net.nextencia.rrdiagram.grammar.model.Repetition;
import net.nextencia.rrdiagram.grammar.model.Rule;
import net.nextencia.rrdiagram.grammar.model.Sequence;
import net.nextencia.rrdiagram.grammar.model.SpecialSequence;
import net.nextencia.rrdiagram.grammar.rrdiagram.RRDiagram;
import net.nextencia.rrdiagram.grammar.rrdiagram.RRDiagramToSVG;
import net.nextencia.rrdiagram.grammar.rrdiagram.RRElement;
import net.nextencia.rrdiagram.grammar.rrdiagram.RRText;

import org.elasticsearch.common.util.LazyInitializable;

import java.awt.Font;
import java.awt.FontFormatException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Generates <a href="https://en.wikipedia.org/wiki/Syntax_diagram">railroad diagrams</a> for docs.
 */
public class RailRoadDiagram {
    /**
     * The font to use in the diagrams. This is loaded from the classpath.
     * If we tried to use the built-in font the rendering would be dependent
     * on whatever fonts you have installed. And, since the world can't agree
     * on fonts, that'd be chaos. So, instead, we load Roboto Mono.
     */
    private static final LazyInitializable<Font, IOException> FONT = new LazyInitializable<>(() -> loadFont().deriveFont(20.0F));

    /**
     * Generate a railroad diagram for a function. The output would look like
     * {@code FOO(a, b, c)}.
     */
    static String functionSignature(FunctionDefinition definition) throws IOException {
        List<Expression> expressions = new ArrayList<>();
        expressions.add(new SpecialSequence(definition.name().toUpperCase(Locale.ROOT)));
        expressions.add(new Syntax("("));

        if (definition.name().equals("case")) {
            // CASE is so weird let's just hack this together manually
            Sequence seq = new Sequence(new Literal("condition"), new Syntax(","), new Literal("trueValue"));
            expressions.add(new Repetition(seq, 1, null));
            expressions.add(new Repetition(new Literal("elseValue"), 0, 1));
        } else {
            List<Expression> argExpressions = new ArrayList<>();
            List<EsqlFunctionRegistry.ArgSignature> args = EsqlFunctionRegistry.description(definition).args();
            for (int i = 0; i < args.size(); i++) {
                EsqlFunctionRegistry.ArgSignature arg = args.get(i);
                String argName = arg.name();
                if (arg.variadic) {
                    if (argName.endsWith("...")) {
                        argName = argName.substring(0, argName.length() - 3);
                    }
                    argExpressions.add(new Repetition(new Sequence(new Syntax(","), new Literal(argName)), arg.optional ? 0 : 1, null));
                } else {
                    if (arg.optional) {
                        if (definition.name().equals("bucket")) {
                            // BUCKET requires optional args to be optional together, so we need custom code to do that
                            var nextArg = args.get(++i);
                            assert nextArg.optional();
                            Sequence seq = new Sequence(new Literal(argName), new Syntax(","), new Literal(nextArg.name));
                            argExpressions.add(new Repetition(seq, 0, 1));
                        } else if (i < args.size() - 1 && args.get(i + 1).optional() == false) {
                            // Special case with leading optional args
                            Sequence seq = new Sequence(new Literal(argName), new Syntax(","));
                            argExpressions.add(new Repetition(seq, 0, 1));
                        } else {
                            argExpressions.add(new Repetition(new Literal(argName), 0, 1));
                        }
                    } else {
                        argExpressions.add(new Literal(argName));
                    }
                }
            }
            expressions.addAll(injectCommas(argExpressions, new Syntax(",")));
        }
        expressions.add(new Syntax(")"));
        return toSvg(new Sequence(expressions.toArray(Expression[]::new)));
    }

    public static List<Expression> injectCommas(List<Expression> original, Expression comma) {
        List<Expression> result = new ArrayList<>();
        for (int i = 0; i < original.size(); i++) {
            result.add(original.get(i));
            if (i < original.size() - 1 && hasComma(original.get(i), true) == false && hasComma(original.get(i + 1), false) == false) {
                result.add(comma);
            }
        }
        return result;
    }

    private static boolean hasComma(Expression exp, boolean atEnd) {
        if (exp instanceof Repetition rep && rep.getExpression() instanceof Sequence seq) {
            Expression[] seqExp = seq.getExpressions();
            int index = atEnd ? seqExp.length - 1 : 0;
            return seqExp[index] instanceof Syntax syntax && syntax.text.equals(",");
        }
        return false;
    }

    /**
     * Generate a railroad diagram for an infix operator like the binary operators, search operator or cast operator.
     * Example output would look like:
     * <dl>
     *     <dt>Addition (binary operator)</dt>
     *     <dd>{@code lhs + rhs}</dd>
     *     <dt>Search</dt>
     *     <dd>{@code field : query}</dd>
     *     <dt>Cast</dt>
     *     <dd>{@code field :: type}</dd>
     * </dl>
     */
    static String infixOperator(String lhs, String operator, String rhs) throws IOException {
        List<Expression> expressions = new ArrayList<>();
        expressions.add(new Literal(lhs));
        expressions.add(new Syntax(operator));
        expressions.add(new Literal(rhs));
        return toSvg(new Sequence(expressions.toArray(Expression[]::new)));
    }

    /**
     * Generate a railroad diagram for prefix operators like the unary operators.
     * For example, for negation the output would look like {@code -v}.
     */
    static String prefixOperator(String operator, String suffix) throws IOException {
        List<Expression> expressions = new ArrayList<>();
        expressions.add(new Syntax(operator));
        expressions.add(new Literal(suffix));
        return toSvg(new Sequence(expressions.toArray(Expression[]::new)));
    }

    /**
     * Generate a railroad diagram for suffix operators like the NULL predicates.
     * For example, for null checks the output would look like {@code field IS NOT NULL}.
     */
    static String suffixOperator(String prefix, String operator) throws IOException {
        List<Expression> expressions = new ArrayList<>();
        expressions.add(new Literal(prefix));
        expressions.add(new Syntax(operator));
        return toSvg(new Sequence(expressions.toArray(Expression[]::new)));
    }

    private static String toSvg(Expression exp) throws IOException {
        RRDiagram rrDiagram = new GrammarToRRDiagram().convert(new Rule("", exp));

        RRDiagramToSVG toSvg = new RRDiagramToSVG();
        toSvg.setSpecialSequenceShape(RRDiagramToSVG.BoxShape.RECTANGLE);
        toSvg.setSpecialSequenceFont(FONT.getOrCompute());

        toSvg.setLiteralFillColor(toSvg.getSpecialSequenceFillColor());
        toSvg.setLiteralFont(FONT.getOrCompute());

        toSvg.setRuleFont(FONT.getOrCompute());
        return toSvg.convert(rrDiagram);
    }

    /**
     * Like a literal but with light grey text for a more muted appearance for syntax.
     */
    private static class Syntax extends Literal {
        private static final String LITERAL_CLASS = "l";
        private static final String SYNTAX_CLASS = "lsyn";
        private static final String LITERAL_TEXT_CLASS = "j";
        private static final String SYNTAX_TEXT_CLASS = "syn";
        private static final String SYNTAX_GREY = "8D8D8D";

        private final String text;

        private Syntax(String text) {
            super(text);
            this.text = text;
        }

        @Override
        protected RRElement toRRElement(GrammarToRRDiagram grammarToRRDiagram) {
            /*
             * This performs a monumentally rude hack to replace the text color of this element.
             * It renders a "literal" element but intercepts the layer that defines it's css class
             * and replaces it with our own.
             */
            return new RRText(RRText.Type.LITERAL, text, null) {
                @Override
                protected void toSVG(RRDiagramToSVG rrDiagramToSVG, int xOffset, int yOffset, RRDiagram.SvgContent svgContent) {
                    super.toSVG(rrDiagramToSVG, xOffset, yOffset, new RRDiagram.SvgContent() {
                        @Override
                        public String getDefinedCSSClass(String style) {
                            if (style.equals(LITERAL_CLASS)) {
                                return svgContent.getDefinedCSSClass(SYNTAX_CLASS);
                            }
                            if (style.equals(LITERAL_TEXT_CLASS)) {
                                return svgContent.getDefinedCSSClass(SYNTAX_TEXT_CLASS);
                            }
                            return svgContent.getDefinedCSSClass(style);
                        }

                        @Override
                        public String setCSSClass(String cssClass, String definition) {
                            if (cssClass.equals(LITERAL_CLASS)) {
                                svgContent.setCSSClass(cssClass, definition);
                                return svgContent.setCSSClass(SYNTAX_CLASS, definition);
                            }
                            if (cssClass.equals(LITERAL_TEXT_CLASS)) {
                                svgContent.setCSSClass(cssClass, definition);
                                return svgContent.setCSSClass(
                                    SYNTAX_TEXT_CLASS,
                                    definition.replace("fill:#000000", "fill:#" + SYNTAX_GREY)
                                );
                            }
                            return svgContent.setCSSClass(cssClass, definition);
                        }

                        @Override
                        public void addPathConnector(int x1, int y1, String path, int x2, int y2) {
                            svgContent.addPathConnector(x1, y1, path, x2, y2);
                        }

                        @Override
                        public void addLineConnector(int x1, int y1, int x2, int y2) {
                            svgContent.addLineConnector(x1, y1, x2, y2);
                        }

                        @Override
                        public void addElement(String element) {
                            svgContent.addElement(element);
                        }
                    });
                }
            };
        }
    }

    private static Font loadFont() throws IOException {
        try {
            InputStream woff = RailRoadDiagram.class.getClassLoader()
                .getResourceAsStream("META-INF/resources/webjars/fontsource__roboto-mono/4.5.7/files/roboto-mono-latin-400-normal.woff");
            if (woff == null) {
                throw new IllegalArgumentException("can't find roboto mono");
            }
            return Font.createFont(Font.TRUETYPE_FONT, new WoffConverter().convertToTTFOutputStream(woff));
        } catch (FontFormatException e) {
            throw new IOException(e);
        }
    }
}
