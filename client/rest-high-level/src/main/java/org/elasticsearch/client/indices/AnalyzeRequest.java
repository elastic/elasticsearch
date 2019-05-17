package org.elasticsearch.client.indices;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A request to analyze text
 */
public class AnalyzeRequest implements Validatable, ToXContentObject {

    private String index;

    private String[] text;

    private String analyzer;

    private NameOrDefinition tokenizer;

    private final List<NameOrDefinition> tokenFilters = new ArrayList<>();

    private final List<NameOrDefinition> charFilters = new ArrayList<>();

    private String field;

    private boolean explain = false;

    private String[] attributes = Strings.EMPTY_ARRAY;

    private String normalizer;

    public static class NameOrDefinition implements ToXContentFragment {
        // exactly one of these two members is not null
        public final String name;
        public final Settings definition;

        NameOrDefinition(String name) {
            this.name = Objects.requireNonNull(name);
            this.definition = null;
        }

        NameOrDefinition(Map<String, ?> definition) {
            this.name = null;
            Objects.requireNonNull(definition);
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.map(definition);
                this.definition = Settings.builder().loadFromSource(Strings.toString(builder), builder.contentType()).build();
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to parse [" + definition + "]", e);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (definition == null) {
                return builder.value(name);
            }
            return definition.toXContent(builder, params);
        }

    }

    public AnalyzeRequest() {
    }

    /**
     * Constructs a new analyzer request for the provided index.
     *
     * @param index The text to analyze
     */
    public AnalyzeRequest(String index) {
        this.index = index;
    }

    /**
     * Set the index that the request should be run against
     */
    public AnalyzeRequest index(String index) {
        this.index = index;
        return this;
    }

    /**
     * Returns the index that the request should be executed against, or {@code null} if
     * no index is specified
     */
    public String index() {
        return this.index;
    }

    /**
     * Returns the text to be analyzed
     */
    public String[] text() {
        return this.text;
    }

    /**
     * Set the text to be analyzed
     */
    public AnalyzeRequest text(String... text) {
        this.text = text;
        return this;
    }

    /**
     * Use a defined analyzer
     */
    public AnalyzeRequest analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public String analyzer() {
        return this.analyzer;
    }

    public AnalyzeRequest tokenizer(String tokenizer) {
        this.tokenizer = new NameOrDefinition(tokenizer);
        return this;
    }

    public AnalyzeRequest tokenizer(Map<String, ?> tokenizer) {
        this.tokenizer = new NameOrDefinition(tokenizer);
        return this;
    }

    public NameOrDefinition tokenizer() {
        return this.tokenizer;
    }

    public AnalyzeRequest addTokenFilter(String tokenFilter) {
        this.tokenFilters.add(new NameOrDefinition(tokenFilter));
        return this;
    }

    public AnalyzeRequest addTokenFilter(Map<String, ?> tokenFilter) {
        this.tokenFilters.add(new NameOrDefinition(tokenFilter));
        return this;
    }

    public List<NameOrDefinition> tokenFilters() {
        return this.tokenFilters;
    }

    public AnalyzeRequest addCharFilter(Map<String, ?> charFilter) {
        this.charFilters.add(new NameOrDefinition(charFilter));
        return this;
    }

    public AnalyzeRequest addCharFilter(String charFilter) {
        this.charFilters.add(new NameOrDefinition(charFilter));
        return this;
    }

    public List<NameOrDefinition> charFilters() {
        return this.charFilters;
    }

    public AnalyzeRequest field(String field) {
        this.field = field;
        return this;
    }

    public String field() {
        return this.field;
    }

    public AnalyzeRequest explain(boolean explain) {
        this.explain = explain;
        return this;
    }

    public boolean explain() {
        return this.explain;
    }

    public AnalyzeRequest attributes(String... attributes) {
        if (attributes == null) {
            throw new IllegalArgumentException("attributes must not be null");
        }
        this.attributes = attributes;
        return this;
    }

    public String[] attributes() {
        return this.attributes;
    }

    public String normalizer() {
        return this.normalizer;
    }

    public AnalyzeRequest normalizer(String normalizer) {
        this.normalizer = normalizer;
        return this;
    }

    @Override
    public Optional<ValidationException> validate() {
        final ValidationException validationException = new ValidationException();
        if (text == null || text.length == 0) {
            validationException.addValidationError("text is missing");
        }
        if ((index == null || index.length() == 0) && normalizer != null) {
            validationException.addValidationError("index is required if normalizer is specified");
        }
        if (normalizer != null && (tokenizer != null || analyzer != null)) {
            validationException.addValidationError("tokenizer/analyze should be null if normalizer is specified");
        }
        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(validationException);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("text", text);
        if (Strings.isNullOrEmpty(analyzer) == false) {
            builder.field("analyzer", analyzer);
        }
        if (tokenizer != null) {
            tokenizer.toXContent(builder, params);
        }
        if (tokenFilters.size() > 0) {
            builder.field("filter", tokenFilters);
        }
        if (charFilters.size() > 0) {
            builder.field("char_filter", charFilters);
        }
        if (Strings.isNullOrEmpty(field) == false) {
            builder.field("field", field);
        }
        if (explain) {
            builder.field("explain", true);
        }
        if (attributes.length > 0) {
            builder.field("attributes", attributes);
        }
        if (Strings.isNullOrEmpty(normalizer) == false) {
            builder.field("normalizer", normalizer);
        }
        return builder.endObject();
    }
}
