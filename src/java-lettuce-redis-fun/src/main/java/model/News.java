package model;

import java.util.StringJoiner;

public class News {
   String link;
   Integer score;

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", News.class.getSimpleName() + "[", "]")
                .add("link='" + link + "'")
                .add("score=" + score)
                .toString();
    }
}
