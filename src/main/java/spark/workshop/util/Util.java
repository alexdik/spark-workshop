package spark.workshop.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {
    public static Iterator<String> tokenize(String str) {
        Pattern pattern = Pattern.compile("\\w+");
        Matcher matcher = pattern.matcher(str);
        List<String> tokens = new ArrayList<>();
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
        return tokens.iterator();
    }
}
