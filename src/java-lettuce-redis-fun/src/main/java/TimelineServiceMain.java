import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import model.News;
import model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class TimelineServiceMain {

    private static final long START_SCORE = 0;
    private static final long MAX_SCORE = 100;
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static RedisClient redisClient = RedisClient.create("redis://localhost/0");
    private static StatefulRedisConnection<String, String> connection = redisClient.connect();
    public static void main(String[] args) throws JsonProcessingException, IOException {
        System.out.println("Connected to Redis");

        //create profiles
        User profile1 = buildProfile1();
        setProfile(profile1);

        User profile2 = buildProfile2();
        setProfile(profile2);

        User profile3 = buildProfile3();
        setProfile(profile3);

        // followers
        addFollowing(profile1.getEmail(), profile2.getEmail());
        addFollowing(profile1.getEmail(), profile3.getEmail());

        addFollowing(profile2.getEmail(), profile1.getEmail());
        addFollowing(profile2.getEmail(), profile3.getEmail());


        // news feed
        addNewsFeed(profile1.getEmail(), buildNews("http://ig.com.br/news/formula-1.html", 4));
        addNewsFeed(profile1.getEmail(), buildNews("http://estadao.com.br/news/esporte-gremio.html", 1));
        addNewsFeed(profile1.getEmail(), buildNews("http://correiobrasilense.com.br/news/formula-1000.html", 10));

        addNewsFeed(profile2.getEmail(), buildNews("http://zerohora.com.br/news/formula-1.html", 4));
        addNewsFeed(profile2.getEmail(), buildNews("http://maskate.com.br/news/esporte-international.html", 1));

        addNewsFeed(profile3.getEmail(), buildNews("AAA", 3));
        addNewsFeed(profile3.getEmail(), buildNews("BBB", 15));

        //timeline
        buildTimeline(profile1.getEmail());
        buildTimeline(profile2.getEmail());

        //print timeline
        System.out.println("\nProfile 1 timeline:");
        getTimeline(profile1.getEmail()).forEach(System.out::println);

        System.out.println("\nProfile 2 timeline:");
        getTimeline(profile2.getEmail()).forEach(System.out::println);

        connection.close();
        redisClient.shutdown();
    }

    private static void buildTimeline(String emailKey) throws IOException {
        delNewsToTimeline(emailKey);

        List<String> followings = getFollowings(emailKey);

        for(String following: followings) {
            List<News> newsList = getNewsFeed(following);
            for (News newsItem : newsList) {
                addNewsToTimeline(emailKey, newsItem);
            }
        }
    }

    private static News buildNews(String link, Integer score) {
        News news = new News();
        news.setLink(link);
        news.setScore(score);

        return news;
    }

    private static User buildProfile1() {
        User user = new User();
        user.setName("Gustavo");
        user.setEmail("gustavo@graeff.com");
        user.setTwitter("twitterGG");
        user.setDateOfBirth("11/09/1998");
        return user;
    }

    private static User buildProfile2() {
        User user = new User();
        user.setName("Karane");
        user.setEmail("karane@vieiracorp.com");
        user.setTwitter("@karane.vieira");
        user.setDateOfBirth("18/02/1992");
        return user;
    }

    private static User buildProfile3() {
        User user = new User();
        user.setName("Nathy");
        user.setEmail("nathy@vieiracorp.com");
        user.setTwitter("@nathy.crestany");
        user.setDateOfBirth("18/12/1992");
        return user;
    }

    private static void addFollowing(String emailKey, String followingEmail) {
        connection.sync().rpush("following:" + emailKey, followingEmail);
    }

    private static List<String> getFollowings(String emailKey) {
        return connection.sync().lrange("following:" + emailKey, 0, -1);
    }

    private static void setProfile(User user) throws JsonProcessingException, IOException {
        String serializedUser = objectMapper.writeValueAsString(user);
        connection.sync().set(user.getEmail(), serializedUser);
    }

    private static User getProfile(String emailKey) throws JsonProcessingException, IOException {
        String returnedUser = connection.sync().get(emailKey);
        return objectMapper.readValue(returnedUser, User.class);
    }

    private static void addNewsFeed(String emailKey, News news) throws JsonProcessingException, IOException {
        String newsItem = objectMapper.writeValueAsString(news);
        connection.sync().rpush("newsfeed:" + emailKey, newsItem);
    }

    private static List<News> getNewsFeed(String emailKey) throws JsonProcessingException, IOException {
        List<String> newsList = connection.sync().lrange("newsfeed:" + emailKey, START_SCORE, -1);
        List<News> timeline = newsList.stream()
                .map(newsString -> {
                    try {
                        return objectMapper.readValue(newsString, News.class);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .collect(Collectors.toList());

        return timeline;
    }

    private static void delNewsToTimeline(String emailKey) throws JsonProcessingException, IOException {
        connection.sync().del("timeline:" + emailKey);
    }

    private static void addNewsToTimeline(String emailKey, News news) throws JsonProcessingException, IOException {
        connection.sync().zadd("timeline:" + emailKey, news.getScore(), news.getLink());
    }

    private static List<String> getTimeline(String emailKey) throws JsonProcessingException, IOException {
        List<String> newsList = connection.sync().zrange("timeline:" + emailKey, START_SCORE, MAX_SCORE);

        return newsList;
    }
}