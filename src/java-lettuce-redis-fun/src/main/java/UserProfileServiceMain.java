import com.fasterxml.jackson.core.JsonProcessingException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import model.User;

import java.io.IOException;

public class UserProfileServiceMain {
    private static final String NAME = "name";
    private static final String EMAIL = "email";
    private static final String TWITTER = "twitter";
    private static final String DATE_OF_BIRTH = "dateOfBirth";
    private static RedisClient redisClient = RedisClient.create("redis://localhost/0");
    private static StatefulRedisConnection<String, String> connection = redisClient.connect();

    public static void main(String[] args) throws JsonProcessingException, IOException {
        System.out.println("Connected to Redis");

        //create user in Redis
        User user = buildUser();
        setProfile(user);

        System.out.println("\nProfile para " + user.getEmail());
        System.out.println("Name: " +  getName(user.getEmail()));
        System.out.println("Email: " +  getEmail(user.getEmail()));
        System.out.println("Twitter: " +  getTwitter(user.getEmail()));
        System.out.println("Date of Birth: " +  getDateOfBirth(user.getEmail()));


        //change values
        setName(user.getEmail(), "Karane");
        setEmail(user.getEmail());
        setTwitter(user.getEmail(), "@karane.vieira");
        setDateOfBirth(user.getEmail(), "18/02/1992");

        System.out.println("\nProfile para " + user.getEmail());
        System.out.println("Name: " +  getName(user.getEmail()));
        System.out.println("Email: " +  getEmail(user.getEmail()));
        System.out.println("Twitter: " +  getTwitter(user.getEmail()));
        System.out.println("Date of Birth: " +  getDateOfBirth(user.getEmail()));

        connection.close();
        redisClient.shutdown();
    }

    private static User buildUser() {
        User user = new User();
        user.setName("Gustavo");
        user.setEmail("gustavo@graeff.com");
        user.setTwitter("twitterGG");
        user.setDateOfBirth("11/09/1998");
        return user;
    }

    private static void setProfile(User user) {
        connection.sync().hset("user:" + user.getEmail(), NAME, user.getName());
        connection.sync().hset("user:" + user.getEmail(), EMAIL, user.getEmail());
        connection.sync().hset("user:" + user.getEmail(), TWITTER, user.getTwitter());
        connection.sync().hset("user:" + user.getEmail(), DATE_OF_BIRTH, user.getDateOfBirth());
    }

    private static String getName(String emailKey) {
        return connection.sync().hget("user:" + emailKey, NAME);
    }

    private static String getEmail(String emailKey) {
        return connection.sync().hget("user:" + emailKey, EMAIL);
    }

    private static String getTwitter(String emailKey) {
        return connection.sync().hget("user:" + emailKey, TWITTER);
    }

    private static String getDateOfBirth(String emailKey) {
        return connection.sync().hget("user:" + emailKey, DATE_OF_BIRTH);
    }

    private static void setName(String emailKey, String name) {
        connection.sync().hset("user:" + emailKey, NAME, name);
    }

    private static void setEmail(String emailKey) {
        connection.sync().hset("user:" + emailKey, EMAIL, emailKey);
    }

    private static void setTwitter(String emailKey, String twitter) {
        connection.sync().hset("user:" + emailKey, TWITTER, twitter);
    }

    private static void setDateOfBirth(String emailKey, String dateOfBirth) {
        connection.sync().hset("user:" + emailKey, DATE_OF_BIRTH, dateOfBirth);
    }

}