package graphql.parser;

import graphql.Internal;
import graphql.language.Document;
import graphql.parser.antlr.GraphqlLexer;
import graphql.parser.antlr.GraphqlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.List;

@Internal
public class Parser {

    public Document parseDocument(String input) throws InvalidSyntaxException {
        return parseDocument(input, null);
    }

    public Document parseDocument(String input, String sourceName) throws InvalidSyntaxException {
        MultiSourceReader multiSourceReader = MultiSourceReader.newMultiSourceReader()
                .string(input, sourceName)
                .trackData(true)
                .build();
        return parseDocument(multiSourceReader);
    }

    public Document parseDocument(Reader reader) throws InvalidSyntaxException {
        MultiSourceReader multiSourceReader;
        if (reader instanceof MultiSourceReader) {
            multiSourceReader = (MultiSourceReader) reader;
        } else {
            multiSourceReader = MultiSourceReader.newMultiSourceReader()
                    .reader(reader, null).build();
        }
        CodePointCharStream charStream;
        try {
            charStream = CharStreams.fromReader(multiSourceReader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        GraphqlLexer lexer = new GraphqlLexer(charStream);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        GraphqlParser parser = new GraphqlParser(tokens);
        parser.removeErrorListeners();
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

        ExtendedBailStrategy bailStrategy = new ExtendedBailStrategy(multiSourceReader);
        parser.setErrorHandler(bailStrategy);

        GraphqlAntlrToLanguage toLanguage = new GraphqlAntlrToLanguage(tokens, multiSourceReader);
        GraphqlParser.DocumentContext documentContext = parser.document();

        Document doc = toLanguage.createDocument(documentContext);

        Token stop = documentContext.getStop();
        List<Token> allTokens = tokens.getTokens();
        if (stop != null && allTokens != null && !allTokens.isEmpty()) {
            Token last = allTokens.get(allTokens.size() - 1);
            //
            // do we have more tokens in the stream than we consumed in the parse?
            // if yes then its invalid.  We make sure its the same channel
            boolean notEOF = last.getType() != Token.EOF;
            boolean lastGreaterThanDocument = last.getTokenIndex() > stop.getTokenIndex();
            boolean sameChannel = last.getChannel() == stop.getChannel();
            if (notEOF && lastGreaterThanDocument && sameChannel) {
                throw bailStrategy.mkMoreTokensException(last);
            }
        }
        return doc;
    }

}
