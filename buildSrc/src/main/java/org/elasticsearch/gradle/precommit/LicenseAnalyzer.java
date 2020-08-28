/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.precommit;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.regex.Pattern;

public class LicenseAnalyzer {
    /*
     * Order here matters. License files can often contain multiple licenses for which the particular piece of software may by used under.
     * We should order these in order of most permissive to least permissive such that we identify the license as the most permissive for
     * purposes of redistribution. Search order is as defined below so the license will be identified as the first pattern to match.
     */
    private static final LicenseMatcher[] matchers = new LicenseMatcher[] {
        new LicenseMatcher("Apache-2.0", true, false, Pattern.compile("Apache.*License.*[vV]ersion.*2\\.0", Pattern.DOTALL)),
        new LicenseMatcher(
            "BSD-2-Clause",
            true,
            false,
            Pattern.compile(
                ("Redistribution and use in source and binary forms, with or without\n"
                    + "modification, are permitted provided that the following conditions\n"
                    + "are met:\n"
                    + "\n"
                    + " 1\\. Redistributions of source code must retain the above copyright\n"
                    + "    notice, this list of conditions and the following disclaimer\\.\n"
                    + " 2\\. Redistributions in binary form must reproduce the above copyright\n"
                    + "    notice, this list of conditions and the following disclaimer in the\n"
                    + "    documentation and/or other materials provided with the distribution\\.\n"
                    + "\n"
                    + "THIS SOFTWARE IS PROVIDED BY .+ (``|''|\")AS IS(''|\") AND ANY EXPRESS OR\n"
                    + "IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n"
                    + "OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED\\.\n"
                    + "IN NO EVENT SHALL .+ BE LIABLE FOR ANY DIRECT, INDIRECT,\n"
                    + "INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES \\(INCLUDING, BUT\n"
                    + "NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
                    + "DATA, OR PROFITS; OR BUSINESS INTERRUPTION\\) HOWEVER CAUSED AND ON ANY\n"
                    + "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
                    + "\\(INCLUDING NEGLIGENCE OR OTHERWISE\\) ARISING IN ANY WAY OUT OF THE USE OF\n"
                    + "THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE\\.").replaceAll("\\s+", "\\\\s*"),
                Pattern.DOTALL
            )
        ),
        new LicenseMatcher(
            "BSD-3-Clause",
            true,
            false,
            Pattern.compile(
                ("\n"
                    + "Redistribution and use in source and binary forms, with or without\n"
                    + "modification, are permitted provided that the following conditions\n"
                    + "are met:\n"
                    + "\n"
                    + " (1\\.)? Redistributions of source code must retain the above copyright\n"
                    + "    notice, this list of conditions and the following disclaimer\\.\n"
                    + " (2\\.)? Redistributions in binary form must reproduce the above copyright\n"
                    + "    notice, this list of conditions and the following disclaimer in the\n"
                    + "    documentation and/or other materials provided with the distribution\\.\n"
                    + " ((3\\.)? The name of .+ may not be used to endorse or promote products\n"
                    + "    derived from this software without specific prior written permission\\.|\n"
                    + "  (3\\.)? Neither the name of .+ nor the names of its\n"
                    + "     contributors may be used to endorse or promote products derived from\n"
                    + "     this software without specific prior written permission\\.)\n"
                    + "\n"
                    + "THIS SOFTWARE IS PROVIDED BY .+ (``|''|\")AS IS(''|\") AND ANY EXPRESS OR\n"
                    + "IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n"
                    + "OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED\\.\n"
                    + "IN NO EVENT SHALL .+ BE LIABLE FOR ANY DIRECT, INDIRECT,\n"
                    + "INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES \\(INCLUDING, BUT\n"
                    + "NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
                    + "DATA, OR PROFITS; OR BUSINESS INTERRUPTION\\) HOWEVER CAUSED AND ON ANY\n"
                    + "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
                    + "\\(INCLUDING NEGLIGENCE OR OTHERWISE\\) ARISING IN ANY WAY OUT OF THE USE OF\n"
                    + "THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE\\.\n").replaceAll("\\s+", "\\\\s*"),
                Pattern.DOTALL
            )
        ),
        new LicenseMatcher(
            "CDDL-1.0",
            true,
            false,
            Pattern.compile("COMMON DEVELOPMENT AND DISTRIBUTION LICENSE.*Version 1.0", Pattern.DOTALL)
        ),
        new LicenseMatcher(
            "CDDL-1.1",
            true,
            false,
            Pattern.compile("COMMON DEVELOPMENT AND DISTRIBUTION LICENSE.*Version 1.1", Pattern.DOTALL)
        ),
        new LicenseMatcher("ICU", true, false, Pattern.compile("ICU License - ICU 1.8.1 and later", Pattern.DOTALL)),
        new LicenseMatcher(
            "MIT",
            true,
            false,
            Pattern.compile(
                ("\n"
                    + "Permission is hereby granted, free of charge, to any person obtaining a copy of\n"
                    + "this software and associated documentation files \\(the \"Software\"\\), to deal in\n"
                    + "the Software without restriction, including without limitation the rights to\n"
                    + "use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies\n"
                    + "of the Software, and to permit persons to whom the Software is furnished to do\n"
                    + "so, subject to the following conditions:\n"
                    + "\n"
                    + "The above copyright notice and this permission notice shall be included in all\n"
                    + "copies or substantial portions of the Software\\.\n"
                    + "\n"
                    + "THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR\n"
                    + "IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,\n"
                    + "FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT\\. IN NO EVENT SHALL THE\n"
                    + "AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER\n"
                    + "LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,\n"
                    + "OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE\n"
                    + "SOFTWARE\\.\n").replaceAll("\\s+", "\\\\s*"),
                Pattern.DOTALL
            )
        ),
        new LicenseMatcher("MPL-1.1", true, false, Pattern.compile("Mozilla Public License.*Version 1.1", Pattern.DOTALL)),
        new LicenseMatcher("MPL-2.0", true, false, Pattern.compile("Mozilla\\s*Public\\s*License\\s*Version\\s*2\\.0", Pattern.DOTALL)),
        new LicenseMatcher("XZ", false, false, Pattern.compile("Licensing of XZ for Java", Pattern.DOTALL)),
        new LicenseMatcher("EPL-2.0", true, false, Pattern.compile("Eclipse Public License - v 2.0", Pattern.DOTALL)),
        new LicenseMatcher("LGPL-2.1", true, true, Pattern.compile("GNU LESSER GENERAL PUBLIC LICENSE.*Version 2.1", Pattern.DOTALL)),
        new LicenseMatcher("LGPL-3.0", true, true, Pattern.compile("GNU LESSER GENERAL PUBLIC LICENSE.*Version 3", Pattern.DOTALL)) };

    public static LicenseInfo licenseType(File licenseFile) {
        for (LicenseMatcher matcher : matchers) {
            boolean matches = matcher.matches(licenseFile);
            if (matches) {
                return new LicenseInfo(matcher.getIdentifier(), matcher.spdxLicense, matcher.sourceRedistributionRequired);
            }
        }

        throw new IllegalStateException("Unknown license for license file: " + licenseFile);
    }

    public static class LicenseInfo {
        private final String identifier;
        private final boolean spdxLicense;
        private final boolean sourceRedistributionRequired;

        public LicenseInfo(String identifier, boolean spdxLicense, boolean sourceRedistributionRequired) {
            this.identifier = identifier;
            this.spdxLicense = spdxLicense;
            this.sourceRedistributionRequired = sourceRedistributionRequired;
        }

        public String getIdentifier() {
            return identifier;
        }

        public boolean isSpdxLicense() {
            return spdxLicense;
        }

        public boolean isSourceRedistributionRequired() {
            return sourceRedistributionRequired;
        }
    }

    private static class LicenseMatcher {
        private final String identifier;
        private final boolean spdxLicense;
        private final boolean sourceRedistributionRequired;
        private final Pattern pattern;

        LicenseMatcher(String identifier, boolean spdxLicense, boolean sourceRedistributionRequired, Pattern pattern) {
            this.identifier = identifier;
            this.spdxLicense = spdxLicense;
            this.sourceRedistributionRequired = sourceRedistributionRequired;
            this.pattern = pattern;
        }

        public String getIdentifier() {
            return identifier;
        }

        public boolean matches(File licenseFile) {
            try {
                String content = Files.readString(licenseFile.toPath()).replaceAll("\\*", " ");
                return pattern.matcher(content).find();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
