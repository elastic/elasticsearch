/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.analysis;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.collation.ICUCollationKeyFilter;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.Locale;

/**
 * An ICU based collation token filter. There are two ways to configure collation:
 *
 * <p>The first is simply specifying the locale (defaults to the default locale). The <tt>language</tt>
 * parameter is the lowercase two-letter ISO-639 code. An additional <tt>country</tt> and <tt>variant</tt>
 * can be provided.
 *
 * <p>The second option is to specify collation rules as defined in the <a href="http://www.icu-project.org/userguide/Collate_Customization.html">
 * Collation customization</a> chapter in icu docs. The <tt>rules</tt> parameter can either embed the rules definition
 * in the settings or refer to an external location (preferable located under the <tt>config</tt> location, relative to it).
 *
 * @author kimchy (shay.banon)
 */
public class IcuCollationTokenFilterFactory extends AbstractTokenFilterFactory {

    private final Collator collator;

    @Inject public IcuCollationTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, Environment environment, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);

        Collator collator;
        String rules = settings.get("rules");
        if (rules != null) {
            FailedToResolveConfigException failureToResolve = null;
            try {
                rules = environment.resolveConfigAndLoadToString(rules);
            } catch (FailedToResolveConfigException e) {
                failureToResolve = e;
            } catch (IOException e) {
                throw new ElasticSearchIllegalArgumentException("Failed to load collation rules", e);
            }
            try {
                collator = new RuleBasedCollator(rules);
            } catch (Exception e) {
                if (failureToResolve != null) {
                    throw new ElasticSearchIllegalArgumentException("Failed to resolve collation rules location", failureToResolve);
                } else {
                    throw new ElasticSearchIllegalArgumentException("Failed to parse collation rules", e);
                }
            }
        } else {
            String language = settings.get("language");
            if (language != null) {
                Locale locale;
                String country = settings.get("country");
                if (country != null) {
                    String variant = settings.get("variant");
                    if (variant != null) {
                        locale = new Locale(language, country, variant);
                    } else {
                        locale = new Locale(language, country);
                    }
                } else {
                    locale = new Locale(language);
                }
                collator = Collator.getInstance(locale);
            } else {
                collator = Collator.getInstance();
            }
        }
        this.collator = collator;
    }

    @Override public TokenStream create(TokenStream tokenStream) {
        return new ICUCollationKeyFilter(tokenStream, collator);
    }
}
