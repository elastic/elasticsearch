/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    private final static String[] HAASE_VARIATIONS_PATTERNS = {"OWN", "RB", "WSK", "A$", "O$", "SCH",
        "GLI", "EAU$", "^CH", "AUX", "EUX", "ILLE"};
    private final static String[] HAASE_VARIATIONS_REPLACEMENTS = {"AUN", "RW", "RSK", "AR", "OW", "CH",
        "LI", "O", "SCH", "O", "O", "I"};

    /**
     *
     * @return
     */
    @Override
    protected String[] getPatterns() {
        return HAASE_VARIATIONS_PATTERNS;
    }

    /**
     * 
     * @return
     */
    @Override
    protected String[] getReplacements() {
        return HAASE_VARIATIONS_REPLACEMENTS;
    }

    /**
     *
     * @return
     */
    @Override
    protected char getCode() {
        return '9';
    }
}
