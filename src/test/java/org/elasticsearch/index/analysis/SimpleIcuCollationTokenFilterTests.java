package org.elasticsearch.index.analysis;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

// Tests borrowed from Solr's Icu collation key filter factory test.
public class SimpleIcuCollationTokenFilterTests {

    /*
    * Turkish has some funny casing.
    * This test shows how you can solve this kind of thing easily with collation.
    * Instead of using LowerCaseFilter, use a turkish collator with primary strength.
    * Then things will sort and match correctly.
    */
    @Test
    public void testBasicUsage() throws Exception {
        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.myCollator.type", "icu_collation")
                .put("index.analysis.filter.myCollator.language", "tr")
                .put("index.analysis.filter.myCollator.strength", "primary")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        String turkishUpperCase = "I WİLL USE TURKİSH CASING";
        String turkishLowerCase = "ı will use turkish casıng";
        TokenFilterFactory filterFactory = analysisService.tokenFilter("myCollator");

        TokenStream tsUpper = filterFactory.create(new KeywordTokenizer(new StringReader(turkishUpperCase)));
        TokenStream tsLower = filterFactory.create(new KeywordTokenizer(new StringReader(turkishLowerCase)));
        assertCollatesToSame(tsUpper, tsLower);
    }

    /*
    * Test usage of the decomposition option for unicode normalization.
    */
    @Test
    public void testNormalization() throws IOException {
        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.myCollator.type", "icu_collation")
                .put("index.analysis.filter.myCollator.language", "tr")
                .put("index.analysis.filter.myCollator.strength", "primary")
                .put("index.analysis.filter.myCollator.decomposition", "canonical")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        String turkishUpperCase = "I W\u0049\u0307LL USE TURKİSH CASING";
        String turkishLowerCase = "ı will use turkish casıng";
        TokenFilterFactory filterFactory = analysisService.tokenFilter("myCollator");

        TokenStream tsUpper = filterFactory.create(new KeywordTokenizer(new StringReader(turkishUpperCase)));
        TokenStream tsLower = filterFactory.create(new KeywordTokenizer(new StringReader(turkishLowerCase)));
        assertCollatesToSame(tsUpper, tsLower);
    }

    /*
    * Test secondary strength, for english case is not significant.
    */
    @Test
    public void testSecondaryStrength() throws IOException {
        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.myCollator.type", "icu_collation")
                .put("index.analysis.filter.myCollator.language", "en")
                .put("index.analysis.filter.myCollator.strength", "secondary")
                .put("index.analysis.filter.myCollator.decomposition", "no")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        String upperCase = "TESTING";
        String lowerCase = "testing";
        TokenFilterFactory filterFactory = analysisService.tokenFilter("myCollator");

        TokenStream tsUpper = filterFactory.create(new KeywordTokenizer(new StringReader(upperCase)));
        TokenStream tsLower = filterFactory.create(new KeywordTokenizer(new StringReader(lowerCase)));
        assertCollatesToSame(tsUpper, tsLower);
    }

    /*
    * Setting alternate=shifted to shift whitespace, punctuation and symbols
    * to quaternary level
    */
    @Test
    public void testIgnorePunctuation() throws IOException {
        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.myCollator.type", "icu_collation")
                .put("index.analysis.filter.myCollator.language", "en")
                .put("index.analysis.filter.myCollator.strength", "primary")
                .put("index.analysis.filter.myCollator.alternate", "shifted")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        String withPunctuation = "foo-bar";
        String withoutPunctuation = "foo bar";
        TokenFilterFactory filterFactory = analysisService.tokenFilter("myCollator");

        TokenStream tsPunctuation = filterFactory.create(new KeywordTokenizer(new StringReader(withPunctuation)));
        TokenStream tsWithoutPunctuation = filterFactory.create(new KeywordTokenizer(new StringReader(withoutPunctuation)));
        assertCollatesToSame(tsPunctuation, tsWithoutPunctuation);
    }

    /*
    * Setting alternate=shifted and variableTop to shift whitespace, but not
    * punctuation or symbols, to quaternary level
    */
    @Test
    public void testIgnoreWhitespace() throws IOException {
        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.myCollator.type", "icu_collation")
                .put("index.analysis.filter.myCollator.language", "en")
                .put("index.analysis.filter.myCollator.strength", "primary")
                .put("index.analysis.filter.myCollator.alternate", "shifted")
                .put("index.analysis.filter.myCollator.variableTop", " ")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        String withSpace = "foo bar";
        String withoutSpace = "foobar";
        String withPunctuation = "foo-bar";
        TokenFilterFactory filterFactory = analysisService.tokenFilter("myCollator");

        TokenStream tsWithSpace = filterFactory.create(new KeywordTokenizer(new StringReader(withSpace)));
        TokenStream tsWithoutSpace = filterFactory.create(new KeywordTokenizer(new StringReader(withoutSpace)));
        assertCollatesToSame(tsWithSpace, tsWithoutSpace);
        // now assert that punctuation still matters: foo-bar < foo bar
        tsWithSpace = filterFactory.create(new KeywordTokenizer(new StringReader(withSpace)));
        TokenStream tsWithPunctuation = filterFactory.create(new KeywordTokenizer(new StringReader(withPunctuation)));
        assertCollation(tsWithPunctuation, tsWithSpace, -1);
    }

    /*
    * Setting numeric to encode digits with numeric value, so that
    * foobar-9 sorts before foobar-10
    */
    @Test
    public void testNumerics() throws IOException {
        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.myCollator.type", "icu_collation")
                .put("index.analysis.filter.myCollator.language", "en")
                .put("index.analysis.filter.myCollator.numeric", "true")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        String nine = "foobar-9";
        String ten = "foobar-10";
        TokenFilterFactory filterFactory = analysisService.tokenFilter("myCollator");

        TokenStream tsNine = filterFactory.create(new KeywordTokenizer(new StringReader(nine)));
        TokenStream tsTen = filterFactory.create(new KeywordTokenizer(new StringReader(ten)));
        assertCollation(tsNine, tsTen, -1);
    }

    /*
    * Setting caseLevel=true to create an additional case level between
    * secondary and tertiary
    */
    @Test
    public void testIgnoreAccentsButNotCase() throws IOException {
        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.myCollator.type", "icu_collation")
                .put("index.analysis.filter.myCollator.language", "en")
                .put("index.analysis.filter.myCollator.strength", "primary")
                .put("index.analysis.filter.myCollator.caseLevel", "true")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        String withAccents = "résumé";
        String withoutAccents = "resume";
        String withAccentsUpperCase = "Résumé";
        String withoutAccentsUpperCase = "Resume";
        TokenFilterFactory filterFactory = analysisService.tokenFilter("myCollator");

        TokenStream tsWithAccents = filterFactory.create(new KeywordTokenizer(new StringReader(withAccents)));
        TokenStream tsWithoutAccents = filterFactory.create(new KeywordTokenizer(new StringReader(withoutAccents)));
        assertCollatesToSame(tsWithAccents, tsWithoutAccents);

        TokenStream tsWithAccentsUpperCase = filterFactory.create(new KeywordTokenizer(new StringReader(withAccentsUpperCase)));
        TokenStream tsWithoutAccentsUpperCase = filterFactory.create(new KeywordTokenizer(new StringReader(withoutAccentsUpperCase)));
        assertCollatesToSame(tsWithAccentsUpperCase, tsWithoutAccentsUpperCase);

        // now assert that case still matters: resume < Resume
        TokenStream tsLower = filterFactory.create(new KeywordTokenizer(new StringReader(withoutAccents)));
        TokenStream tsUpper = filterFactory.create(new KeywordTokenizer(new StringReader(withoutAccentsUpperCase)));
        assertCollation(tsLower, tsUpper, -1);
    }

    /*
    * Setting caseFirst=upper to cause uppercase strings to sort
    * before lowercase ones.
    */
    @Test
    public void testUpperCaseFirst() throws IOException {
        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.myCollator.type", "icu_collation")
                .put("index.analysis.filter.myCollator.language", "en")
                .put("index.analysis.filter.myCollator.strength", "tertiary")
                .put("index.analysis.filter.myCollator.caseFirst", "upper")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        String lower = "resume";
        String upper = "Resume";
        TokenFilterFactory filterFactory = analysisService.tokenFilter("myCollator");

        TokenStream tsLower = filterFactory.create(new KeywordTokenizer(new StringReader(lower)));
        TokenStream tsUpper = filterFactory.create(new KeywordTokenizer(new StringReader(upper)));
        assertCollation(tsUpper, tsLower, -1);
    }

    /*
    * For german, you might want oe to sort and match with o umlaut.
    * This is not the default, but you can make a customized ruleset to do this.
    *
    * The default is DIN 5007-1, this shows how to tailor a collator to get DIN 5007-2 behavior.
    *  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4423383
    */
    @Test
    public void testCustomRules() throws Exception {
        RuleBasedCollator baseCollator = (RuleBasedCollator) Collator.getInstance(new ULocale("de_DE"));
        String DIN5007_2_tailorings =
                "& ae , a\u0308 & AE , A\u0308"+
                        "& oe , o\u0308 & OE , O\u0308"+
                        "& ue , u\u0308 & UE , u\u0308";

        RuleBasedCollator tailoredCollator = new RuleBasedCollator(baseCollator.getRules() + DIN5007_2_tailorings);
        String tailoredRules = tailoredCollator.getRules();

        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.myCollator.type", "icu_collation")
                .put("index.analysis.filter.myCollator.rules", tailoredRules)
                .put("index.analysis.filter.myCollator.strength", "primary")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        String germanUmlaut = "Töne";
        String germanOE = "Toene";
        TokenFilterFactory filterFactory = analysisService.tokenFilter("myCollator");
        TokenStream tsUmlaut = filterFactory.create(new KeywordTokenizer(new StringReader(germanUmlaut)));
        TokenStream tsOE = filterFactory.create(new KeywordTokenizer(new StringReader(germanOE)));
        assertCollatesToSame(tsUmlaut, tsOE);
    }

    private AnalysisService createAnalysisService(Index index, Settings settings) {
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings), new EnvironmentModule(new Environment(settings)), new IndicesAnalysisModule()).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class)).addProcessor(new IcuAnalysisBinderProcessor()))
                .createChildInjector(parentInjector);

        return injector.getInstance(AnalysisService.class);
    }

    private void assertCollatesToSame(TokenStream stream1, TokenStream stream2) throws IOException {
        assertCollation(stream1, stream2, 0);
    }

    private void assertCollation(TokenStream stream1, TokenStream stream2, int comparison) throws IOException {
        CharTermAttribute term1 = stream1
                .addAttribute(CharTermAttribute.class);
        CharTermAttribute term2 = stream2
                .addAttribute(CharTermAttribute.class);
        assertThat(stream1.incrementToken(), equalTo(true));
        assertThat(stream2.incrementToken(), equalTo(true));
        assertThat(Integer.signum(term1.toString().compareTo(term2.toString())), equalTo(Integer.signum(comparison)));
        assertThat(stream1.incrementToken(), equalTo(false));
        assertThat(stream2.incrementToken(), equalTo(false));
    }

}
