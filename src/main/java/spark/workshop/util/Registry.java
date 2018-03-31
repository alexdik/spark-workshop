package spark.workshop.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Registry implements Serializable {
    private final Map<Long, String> publishers;

    public Registry() {
        this.publishers = new HashMap<Long, String>() {{
            put(1L, "Pearson");
            put(2L, "RELX Group");
            put(3L, "ThomsonReuters");
            put(4L, "Bertelsmann");
            put(5L, "Wolters Kluwer");
            put(6L, "Hachette Livre");
            put(7L, "Grupo Planeta");
            put(8L, "McGraw-Hill");
            put(9L, "Wiley");
            put(10L, "Springer Nature");
        }};
    }

    public String getPublisherName(long publisherId) {
        return publishers.get(publisherId);
    }

}
