/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis.phonetic;

/**
 * Ge&auml;nderter Algorithmus aus der Matching Toolbox von Rainer Schnell
 * Java-Programmierung von J&ouml;rg Reiher
 *
 * Die Kölner Phonetik wurde für den Einsatz in Namensdatenbanken wie
 * der Verwaltung eines Krankenhauses durch Martin Haase (Institut für
 * Sprachwissenschaft, Universität zu Köln) und Kai Heitmann (Insitut für
 * medizinische Statistik, Informatik und Epidemiologie, Köln)  überarbeitet.
 * M. Haase und K. Heitmann. Die Erweiterte Kölner Phonetik. 526, 2000.
 *
 * nach: Martin Wilz, Aspekte der Kodierung phonetischer Ähnlichkeiten
 * in deutschen Eigennamen, Magisterarbeit.
 * http://www.uni-koeln.de/phil-fak/phonetik/Lehre/MA-Arbeiten/magister_wilz.pdf
 *
 * @author <a href="mailto:joergprante@gmail.com">J&ouml;rg Prante</a>
 */
public class HaasePhonetik extends KoelnerPhonetik {

    private static final String[] HAASE_VARIATIONS_PATTERNS = {"OWN", "RB", "WSK", "A$", "O$", "SCH",
        "GLI", "EAU$", "^CH", "AUX", "EUX", "ILLE"};
    private static final String[] HAASE_VARIATIONS_REPLACEMENTS = {"AUN", "RW", "RSK", "AR", "OW", "CH",
        "LI", "O", "SCH", "O", "O", "I"};

    @Override
    protected String[] getPatterns() {
        return HAASE_VARIATIONS_PATTERNS;
    }

    @Override
    protected String[] getReplacements() {
        return HAASE_VARIATIONS_REPLACEMENTS;
    }

    @Override
    protected char getCode() {
        return '9';
    }
}
