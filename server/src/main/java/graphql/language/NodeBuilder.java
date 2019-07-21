package graphql.language;

import graphql.PublicApi;

import java.util.List;
import java.util.Map;

import static graphql.Assert.assertNotNull;

@PublicApi
public interface NodeBuilder {

    NodeBuilder sourceLocation(SourceLocation sourceLocation);

    NodeBuilder comments(List<Comment> comments);

    NodeBuilder ignoredChars(IgnoredChars ignoredChars);

    NodeBuilder additionalData(Map<String, String> additionalData);

    NodeBuilder additionalData(String key, String value);

}
