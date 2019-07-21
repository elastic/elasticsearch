package graphql.parser;

import graphql.language.SourceLocation;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.List;

import static graphql.parser.SourceLocationHelper.mkSourceLocation;

public class ExtendedBailStrategy extends BailErrorStrategy {
    private final MultiSourceReader multiSourceReader;

    public ExtendedBailStrategy(MultiSourceReader multiSourceReader) {
        this.multiSourceReader = multiSourceReader;
    }

    @Override
    public void recover(Parser recognizer, RecognitionException e) {
        try {
            super.recover(recognizer, e);
        } catch (ParseCancellationException parseException) {
            throw mkException(recognizer, e);
        }
    }

    @Override
    public Token recoverInline(Parser recognizer) throws RecognitionException {
        try {
            return super.recoverInline(recognizer);
        } catch (ParseCancellationException parseException) {
            throw mkException(recognizer, null);
        }
    }

    InvalidSyntaxException mkMoreTokensException(Token token) {
        SourceLocation sourceLocation = mkSourceLocation(multiSourceReader, token);
        String sourcePreview = mkPreview(token);
        return new InvalidSyntaxException(sourceLocation,
                "There are more tokens in the query that have not been consumed",
                sourcePreview, token.getText(), null);
    }


    private InvalidSyntaxException mkException(Parser recognizer, RecognitionException cause) {
        String sourcePreview = null;
        String offendingToken = null;
        SourceLocation sourceLocation = null;
        Token currentToken = recognizer.getCurrentToken();
        if (currentToken != null) {
            sourceLocation = mkSourceLocation(multiSourceReader, currentToken);
            offendingToken = currentToken.getText();
            sourcePreview = mkPreview(currentToken);
        }
        return new InvalidSyntaxException(sourceLocation, null, sourcePreview, offendingToken, cause);
    }

    /* grabs 3 lines before and after the syntax error */
    private String mkPreview(Token token) {
        int line = token.getLine() - 1;
        StringBuilder sb = new StringBuilder();
        int startLine = line - 3;
        int endLine = line + 3;
        List<String> lines = multiSourceReader.getData();
        for (int i = 0; i < lines.size(); i++) {
            if (i >= startLine && i <= endLine) {
                sb.append(lines.get(i)).append('\n');
            }
        }
        return sb.toString();
    }

}
