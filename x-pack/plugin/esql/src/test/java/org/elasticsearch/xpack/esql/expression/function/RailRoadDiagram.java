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

import org.elasticsearch.xpack.esql.plan.logical.show.ShowFunctions;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Generates <a href="https://en.wikipedia.org/wiki/Syntax_diagram">railroad diagrams</a> for docs.
 */
public class RailRoadDiagram {
    static String functionSignature(FunctionDefinition definition) {
        List<Expression> expressions = new ArrayList<>();
        expressions.add(new SpecialSequence(definition.name().toUpperCase(Locale.ROOT)));
        expressions.add(new Syntax("("));
        boolean first = true;
        List<String> args = ShowFunctions.signature(definition);
        for (String arg : args) {
            if (arg.endsWith("...")) {
                expressions.add(new Repetition(new Sequence(new Syntax(","), new Literal(arg.substring(0, arg.length() - 3))), 0, null));
            } else {
                if (first) {
                    first = false;
                } else {
                    expressions.add(new Syntax(","));
                }
                expressions.add(new Literal(arg));
            }
        }
        expressions.add(new Syntax(")"));
        net.nextencia.rrdiagram.grammar.model.Expression rr = new Sequence(
            expressions.toArray(net.nextencia.rrdiagram.grammar.model.Expression[]::new)
        );
        RRDiagram rrDiagram = new GrammarToRRDiagram().convert(new Rule("test", rr));
        RRDiagramToSVG toSvg = new RRDiagramToSVG();
        toSvg.setSpecialSequenceShape(RRDiagramToSVG.BoxShape.RECTANGLE);
        toSvg.setLiteralFillColor(toSvg.getSpecialSequenceFillColor());
        toSvg.setLiteralFont(toSvg.getLiteralFont().deriveFont(20.0F));
        toSvg.setSpecialSequenceFont(toSvg.getSpecialSequenceFont().deriveFont(20.0F));
        toSvg.setRuleFont(toSvg.getRuleFont().deriveFont(20.0F));
        return toSvg.convert(rrDiagram);
    }

    /**
     * Like a literal but with light grey text for a more muted appearance for syntax.
     */
    private static class Syntax extends Literal {
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
            // This performs a monumentally rude hack to replace the text color of this element.
            return new RRText(RRText.Type.LITERAL, text, null) {
                @Override
                protected void toSVG(RRDiagramToSVG rrDiagramToSVG, int xOffset, int yOffset, RRDiagram.SvgContent svgContent) {
                    super.toSVG(rrDiagramToSVG, xOffset, yOffset, new RRDiagram.SvgContent() {
                        @Override
                        public String getDefinedCSSClass(String style) {
                            if (style.equals(LITERAL_TEXT_CLASS)) {
                                return svgContent.getDefinedCSSClass(SYNTAX_TEXT_CLASS);
                            }
                            return svgContent.getDefinedCSSClass(style);
                        }

                        @Override
                        public String setCSSClass(String cssClass, String definition) {
                            if (false == cssClass.equals(LITERAL_TEXT_CLASS)) {
                                return svgContent.setCSSClass(cssClass, definition);
                            }
                            svgContent.setCSSClass(cssClass, definition);
                            return svgContent.setCSSClass(SYNTAX_TEXT_CLASS, definition.replace("fill:#000000", "fill:#" + SYNTAX_GREY));
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
}
