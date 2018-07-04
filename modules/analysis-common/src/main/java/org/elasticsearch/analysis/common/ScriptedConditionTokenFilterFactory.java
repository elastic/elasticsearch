package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ConditionalTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.ReferringFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ScriptedConditionTokenFilterFactory extends AbstractTokenFilterFactory implements ReferringFilterFactory {

    private final ConditionFactory factory;
    private final List<TokenFilterFactory> filters = new ArrayList<>();
    private final List<String> filterNames;

    public ScriptedConditionTokenFilterFactory(IndexSettings indexSettings, String name,
                                               Settings settings, ScriptService scriptService) {
        super(indexSettings, name, settings);

        Settings scriptSettings = settings.getAsSettings("script");
        Script script = Script.parse(scriptSettings);
        if (script.getType() != ScriptType.INLINE) {
            throw new IllegalArgumentException("Cannot use stored scripts in tokenfilter [" + name + "]");
        }
        this.factory = scriptService.compile(script, CONTEXT);

        this.filterNames = settings.getAsList("filters");
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        Function<TokenStream, TokenStream> filter = in -> {
            for (TokenFilterFactory tff : filters) {
                in = tff.create(in);
            }
            return in;
        };
        ConditionScript script = factory.newInstance();
        return new ConditionalTokenFilter(tokenStream, filter) {

            CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
            PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
            OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
            TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

            @Override
            protected boolean shouldFilter() {
                return script.execute(termAtt, posIncAtt.getPositionIncrement(), offsetAtt.startOffset(), offsetAtt.endOffset(),
                    posLenAtt.getPositionLength(), typeAtt.type());
            }
        };
    }

    @Override
    public void setReferences(Map<String, TokenFilterFactory> factories) {
        for (String filter : filterNames) {
            TokenFilterFactory tff = factories.get(filter);
            if (tff == null) {
                throw new IllegalArgumentException("ScriptedConditionTokenFilter [" + name() +
                    "] refers to undefined token filter [" + filter + "]");
            }
            filters.add(tff);
        }
    }

    public static abstract class ConditionScript {

        public abstract boolean execute(CharSequence term, int posInc, int startOffset, int endOffset, int posLen, String type);

    }

    public interface ConditionFactory {
        ConditionScript newInstance();
    }

    public static final String[] PARAMETERS = new String[] {"term", "posInc", "startOffset", "endOffset", "posLen", "type"};
    public static final ScriptContext<ConditionFactory> CONTEXT = new ScriptContext<>("condition", ConditionFactory.class);
}
