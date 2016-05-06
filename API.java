/*
cd src/uk/ac/bris/cs/databases/cwk3
cd ../../../../../../..

ANT ON UNIX - Note for Alex
export ANT_HOME=/Library/Ant
export PATH=${PATH}:${ANT_HOME}/bin
*/

package uk.ac.bris.cs.databases.cwk3;

import java.sql.*;
//import java.util.List;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.sqlite.SQLiteConfig;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import uk.ac.bris.cs.databases.api.*;
import uk.ac.bris.cs.databases.util.Params;
import uk.ac.bris.cs.databases.web.ApplicationContext;

/**
 *
 * @author csxdb
 */
public class API implements APIProvider {

    private final Connection c;

   /**
    * API constructor
    * @param c The connection to the database (ensure Foreign Key Constraints are enabled) 
    */
   public API(Connection c) {
        this.c = c;
    }

    private static final String DATABASE = "jdbc:sqlite:database/database.sqlite3";

   /**
    * <h1>For Running Tests Only</h1>
    * @param args Not used
    */
   public static void main(String[] args){
      //SET UP FOR TESTS
         ApplicationContext c = ApplicationContext.getInstance();
         API api;
         Connection conn;

         try{
            Properties props = new Properties();
            props.setProperty("foreign_keys", "true");
            conn = DriverManager.getConnection(DATABASE, props);
            conn.setAutoCommit(false);

            api = new API(conn);
            c.setApi(api);
         } catch (SQLException e) {
            throw new RuntimeException(e);
         }

      //TESTS
         api.tests();

      //Close connection
       try {
          conn.close();
       } catch (SQLException ex) {
          System.err.println("Can't close DB");
       }
    }

    private void tests(){
      /*
       we should make database/unitTests.sqlite3 and load that instead of
       one that will keep changing as we play with the forum.
      */
      int passed = 0;
      int failed = 0;

      //--getUsers
      if(test(getUsers(), "success")) passed++;
      else {p("Failed getUsers"); failed++; }

      //--getPersonView
      if(test(getPersonView("tb15269"), "success")) passed++;
      else {p("Failed getPersonView 1"); failed++; }

      if(test(getPersonView("tb1269"), "fatal")) passed++;
      else {p("Failed getPersonView 2"); failed++; }

      //--getSimpleForums
      if(test(getSimpleForums(), "success")) passed++;
      else {p("Failed getUsers"); failed++; }

      //--countPostsInTopic
      if(test(countPostsInTopic(1), "success")) passed++;
      else {p("Failed countPostsInTopic"); failed++; }

      //--getLikers
      if(test(getLikers(1), "success")) passed++;
      else {p("Failed getLikers1"); failed++; }

      if(test(getLikers(100), "failure")) passed++;
      else {p("Failed getLikers2"); failed++; }
      
      if(test(getLikers(7), "success")) passed++;
      else {p("Failed getLikers3"); failed++; }

      //--getSimpleTopic
      if(test(getSimpleTopic(1), "success")) passed++;
      else {p("Failed getSimpleTopic1"); failed++; }

      if(test(getSimpleTopic(100), "fatal")) passed++;
      else {p("Failed getSimpleTopic2"); failed++; }

      //--getLatestPost
      if(test(getLatestPost(1), "success")) passed++;
      else {p("Failed getLatestPost1"); failed++; }

      if(test(getLatestPost(100), "failure")) passed++;
      else {p("Failed getLatestPost2"); failed++; }

      //--getForums
      if(test(getForums(), "success")) passed++;
      else {p("Failed getForums"); failed++; }

      //--createForum
      if(test(createForum("test"), "success")) passed++;
      else {p("Failed createForum1 - simple create test"); failed++; }
      deleteForum("test");

      if(test(createForum("Politics"), "failure")) passed++;
      else {p("Failed createForum2 - creating duplicate"); failed++; }

      if(test(createForum(null), "failure")) passed++;
      else {p("Failed createForum3 - create with null title"); failed++; }

      if(test(createForum(""), "failure")) passed++;
      else {p("Failed createForum4 - create with empty title"); failed++; }


      //--createPost
      if(test(createPost(1,"ak15308","testPost"), "success")) passed++;
      else {p("Failed createPost1 - create with valid everything"); failed++; }
      deletePost(3, 1, "testPost");

      if(test(createPost(100,"tb15269","testPost"), "failure")) passed++;
      else {p("Failed createPost2 - Topic does not exist"); failed++; }

      if(test(createPost(1,"tb1529","testPost"), "failure")) passed++;
      else {p("Failed createPost3 - User does not exist"); failed++; }

      if(test(createPost(1,"","testPost"), "failure")) passed++;
      else {p("Failed createPost4 - Empty username"); failed++; }

      if(test(createPost(1,"tb1529",""), "failure")) passed++;
      else {p("Failed createPost5 - Empty text"); failed++; }

      //--addNewPerson(String name, String username, String studentId)
      deletePerson("santababy");
      if(test(addNewPerson("santa", "santababy", "123456789"), "success")) passed++;
      else {p("Failed addNewPerson1"); failed++; }
      //don't delete next tests for the overwrite
      
      if(test(addNewPerson("santa", "santababy", "123456789"), "failure")) passed++;
      else {p("Failed addNewPerson2"); failed++; }
      deletePerson("santababy");
      
      if(test(addNewPerson("", "santababy", "123456789"), "fatal")) passed++;
      else {p("Failed addNewPerson3"); failed++; }
      deletePerson("santababy");
      
      if(test(addNewPerson(null, "santababy", "123456789"), "fatal")) passed++;
      else {p("Failed addNewPerson4"); failed++; }
      deletePerson("santababy");
      
      if(test(addNewPerson("santa", "", "123456789"), "fatal")) passed++;
      else {p("Failed addNewPerson5"); failed++; }
      deletePerson("santababy");
      
      if(test(addNewPerson("santa", null, "123456789"), "fatal")) passed++;
      else {p("Failed addNewPerson6"); failed++; }
      deletePerson("santababy");
      
      if(test(addNewPerson("santa", "santababy", ""), "fatal")) passed++;
      else {p("Failed addNewPerson7"); failed++; }
      deletePerson("santababy");
      
      if(test(addNewPerson("santa", "santababy", null), "success")) passed++;
      else {p("Failed addNewPerson8"); failed++; }
      deletePerson("santababy");
      
      //--getForum(long id)
      if(test(getForum(1), "success")) passed++;
      else {p("Failed getForum1"); failed++; }
      
      if(test(getForum(100), "failure")) passed++;
      else {p("Failed getForum2"); failed++; }
      
      //--getTopic(long topicId, int page)
      if(test(getTopic(4,0), "success")) passed++;
      else {p("Failed getTopic1"); failed++; }
      
      if(test(getTopic(1,2), "failure")) passed++;
      else {p("Failed getTopic2"); failed++; }
      
      //--likeTopic(String username, long topicId, boolean like)
      if(test(likeTopic("noUser", 1, false), "failure")) passed++;
      else {p("Failed likeTopic1"); failed++; }
      
      if(test(likeTopic("jl15351", 3, false), "success")) passed++;
      else {p("Failed likeTopic2"); failed++; }
      
      if(test(likeTopic("ak15308", 1, true), "success")) passed++;
      else {p("Failed likeTopic3"); failed++; }
      
      if(test(likeTopic("jl15351", 7, true), "success")) passed++;
      else {p("Failed likeTopic3"); failed++; }
      
      /*
      
      
      favouriteTopic(String username, long topicId, boolean fav)
      //LEVEL 3*/

      //--createTopic
      //failure if any of the preconditions are not met (forum does not exist, user does not exist, title or text empty);
      if(test(createTopic(1,"tb15269","testTopic", "This is some test text"), "success")) passed++;
      else {p("Failed createTopic1 - create with valid everything"); failed++; }
      deleteTopic(0, "testTopic");

      if(test(createTopic(100,"tb15269","testTopic", "This is some test text"), "failure")) passed++;
      else {p("Failed createTopic2 - Forum does not exist"); failed++; }

      if(test(createTopic(1,"tb1529","testTopic", "This is some test text"), "failure")) passed++;
      else {p("Failed createTopic3 - User does not exist"); failed++; }

      if(test(createTopic(1,"tb1529","", "This is some test text"), "failure")) passed++;
      else {p("Failed createTopic4 - Empty Title"); failed++; }

      if(test(createTopic(1,"tb1529","testTopic", ""), "failure")) passed++;
      else {p("Failed createTopic5 - Empty Text"); failed++; }

      /*getAdvancedForums()
      getAdvancedPersonView(String username)
      getAdvancedForum(long id)
      likePost(String username, long topicId, int post, boolean like)
      */

      p("Passed " + passed + " tests. Failed " + failed);
    }

    private void p(String s){
       System.out.println(s);
    }

    private boolean test(Result r, String expectedResult){
       try{
         switch(expectedResult){
            case "success":
               if(r.isSuccess()) return true;
               return false;
            case "failure":
               if(!r.isFatal()) return true; //isFatal returns false if is failiure, exception if success, true if fatal
               return false;
            case "fatal":
               if(r.isFatal()) return true;
               return false;
            default:
               System.err.println("Test configured incorrectly");
               return false;
         }
       }catch(RuntimeException e){
          return false;
       }
    }

   /**
   * <p>Get a list of all users in the system as a map username -> name.</p>
   * <p><b>Visual Test:</b> http://localhost:8000/people</p>
   * <p><b>Main Contributor:</b> Joseph</p>
   * <p><b>SQL:</b> SELECT username, name FROM Person;</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @return A map with one entry per user of the form username -> name
   * (note that usernames are unique).
   */
   @Override
   public Result<Map<String, String>> getUsers() {
   	try(
   		PreparedStatement s = c.prepareStatement(
               "SELECT username, name FROM Person;"
   		);
         ){
            Map<String, String> map = new HashMap<>();
            ResultSet r = s.executeQuery();
            while(r.next()){
               map.put(r.getString("username"), r.getString("name"));
            }
            return Result.success(map);
   	}catch (SQLException ex) {
         printError("Error in getUsers: " + ex.getMessage());
   	}
   	return Result.fatal("Fatal getUsers");
   }

   /**
   * <p>Get a PersonView for the person with the given username.</p>
   * <p><b>Visual Test:</b> http://localhost:8000/person/tb15269</p>
   * <p><b>Main Contributor:</b> Tom</p>
   * <p><b>SQL:</b> SELECT name, username, stuId FROM Person " + "WHERE username = ?;</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param username the username to search for, cannot be empty.
   * @return If a person with the given username exists, a fully populated
   * PersonView. Otherwise, failure (or fatal on a database error).
   */
   @Override
   public Result<PersonView> getPersonView(String username) {
      Params.cannotBeEmpty(username);
      Params.cannotBeNull(username);
      try(
            PreparedStatement s = c.prepareStatement(
               "SELECT name, username, stuId FROM Person " + "WHERE username = ?;"
            );
         ){
         s.setString(1, username);
         ResultSet r = s.executeQuery();
         PersonView pv = new PersonView(r.getString("name"),
                                        r.getString("username"),
                                        r.getString("stuId"));
         return Result.success(pv);
      }catch (SQLException ex) {
         printError("Error in getPersonView: " + ex.getMessage());
      }
      return Result.fatal("Fatal getPersonView");
   }
   
   /**
   * <p>Get the "main page" containing a list of forums ordered alphabetically
   * by title. Simple version that does not return any topic information.</p>
   * <p><b>Visual Test:</b> http://localhost:8000/forums0</p>
   * <p><b>Main Contributor:</b> Alex</p>
   * <p><b>SQL:</b> SELECT id, title FROM Forum;</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @return the list of all forums; an empty list if there are no forums.
   */
    @Override
    public Result<List<SimpleForumSummaryView>> getSimpleForums() {
      try(
            PreparedStatement s = c.prepareStatement(
               "SELECT id, title FROM Forum;"
            );
         ){
         ResultSet r = s.executeQuery();
         List<SimpleForumSummaryView> simpleForumsList = new ArrayList<>();
         while (r.next()) {
            SimpleForumSummaryView sfsv = new SimpleForumSummaryView(r.getLong("id"),
                                                                     r.getString("title"));
            simpleForumsList.add(sfsv);
         }
         return Result.success(simpleForumsList);
      }catch (SQLException ex) {
         printError("Error in getSimpleForums: " + ex.getMessage());
      }
      return Result.fatal("Fatal getSimpleForums");
    }

   /**
   * <p>Count the number of posts in a topic (without fetching them all).</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> Tom</p>
   * <p><b>SQL:</b> SELECT COUNT(*) AS numposts FROM POST WHERE topicid = ?;</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param topicId the topic to look at.
   * @return The number of posts in this topic if it exists, otherwise a
   * failure.
   */
    @Override
    public Result<Integer> countPostsInTopic(long topicId) {
       try{
         PreparedStatement s = c.prepareStatement(
               "SELECT COUNT(*) AS numposts FROM POST WHERE topicid = ?;"
            );
         s.setLong(1, topicId);
         ResultSet r = s.executeQuery();
         return Result.success(r.getInt("numposts"));
       }catch(SQLException ex){
         printError("Error in getSimpleTopic: " + ex.getMessage());
       }
       return Result.fatal("Fatal getPostsInTopic");
    }

   /**
   * <p>Get all people who have liked a particular topic, ordered by name
   * alphabetically.</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> Joseph</p>
   * <p><b>SQL:</b> SELECT Topic.id, Person.name, Person.username, Person.stuId
   * FROM Topic
   * LEFT OUTER JOIN Topic_Likers ON topic.id = Topic_Likers.topicid
   * LEFT OUTER JOIN Person ON Topic_Likers.personid = Person.id
   * WHERE Topic.id = ? 
   * ORDER BY Person.name ASC;</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param topicId The topic id. Must exist.
   * @return Success (even if the list is empty) if the topic exists,
   * failure if it does not, fatal in case of database errors.
   */
    @Override
    public Result<List<PersonView>> getLikers(long topicId) {
       try(
         PreparedStatement s = c.prepareStatement(
            "SELECT Topic.id, Person.name, Person.username, Person.stuId " +
            "FROM Topic " +
            "LEFT OUTER JOIN Topic_Likers ON topic.id = Topic_Likers.topicid " +
            "LEFT OUTER JOIN Person ON Topic_Likers.personid = Person.id " +
            "WHERE Topic.id = ? " +
            "ORDER BY Person.name ASC;"
   		);
         ){
         s.setLong(1, topicId);
         ResultSet r = s.executeQuery();
         
         List<PersonView> likers = new ArrayList<>();
         if(r.next()){
            if(r.getString("stuId") != null){
               PersonView liker = new PersonView(r.getString("name"), r.getString("username"), r.getString("stuId"));
               likers.add(liker);
            }
         }
         else return Result.failure("Failure in getLikers, topic does not exist");
         
         while(r.next()){
            PersonView liker = new PersonView(r.getString("name"), r.getString("username"), r.getString("stuId"));
            likers.add(liker);
         }
         return Result.success(likers);
   	}catch (SQLException ex) {
         printError("Error in getLikers: " + ex.getMessage());
         return Result.fatal("Fatal getLikers");
   	}
   }

   /**
   * <p>Get a simplified view of a topic.</p>
   * <p><b>Visual Test:</b> http://localhost:8000/topic0/1</p>
   * <p><b>Main Contributor:</b> Alex</p>
   * <p><b>SQL:</b> SELECT t.id as topicid, t.title, p.id as postid, per.username, p.text, p.postedAt FROM Topic AS t JOIN Post AS p ON t.id = p.topicid JOIN Person AS per ON p.authorid = per.id WHERE t.id = 1;</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param topicId the topic to get.
   * @return The topic view if one exists with the given id,
   * otherwise failure or fatal on database errors. 
   */
    @Override
    public Result<SimpleTopicView> getSimpleTopic(long topicId) {
      try(
            PreparedStatement s = c.prepareStatement(
               "SELECT t.id as topicid, t.title, p.id as postid, per.username, p.text, p.postedAt FROM Topic AS t " +
               "JOIN Post AS p ON t.id = p.topicid " +
               "JOIN Person AS per ON p.authorid = per.id " +
               "WHERE t.id = ?"
            );
         ){
         s.setLong(1, topicId);

         ResultSet r = s.executeQuery();

         String topicTitle = r.getString("title");

         //Collect post and add to list
         List<SimplePostView> simplePostsList = new ArrayList<>();
         while (r.next()) {
            SimplePostView spv = new SimplePostView(r.getInt("postid"),
                                                    r.getString("username"),
                                                    r.getString("text"),
                                                    r.getInt("postedAt"));
            simplePostsList.add(spv);
         }

         //Create simpleTopicView pass list of posts
         SimpleTopicView stv = new SimpleTopicView(topicId,
                                                   topicTitle,
                                                   simplePostsList);

         return Result.success(stv);

      }catch (SQLException ex) {
         printError("Error in getSimpleTopic: " + ex.getMessage());
      }
      return Result.fatal("Fatal getSimpleTopic");
    }

   /**
   * <h1>THIS NEEDS WORK - NEVER RETURNS FAILURE DOESN'T CHECK FOR TOPIC EXISTS</h1>
   * <p>Get the latest post in a topic.</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> Tom</p>
   * <p><b>SQL:</b> SELECT forum.id as forumid, post.topicid as topicid, 
   *post.id as postNumber, person.name as authorname, 
   *person.username as username, post.text as text, 
   *post.postedAt as postedAt, likes.numLikes as numberOfLikes 
   *FROM Post 
   *JOIN Person ON Post.authorid = Person.id 
   *JOIN Topic ON Post.topicid = Topic.id 
   *JOIN Forum ON Topic.forumid = Forum.id 
   *JOIN (SELECT postid, count(*) as numLikes FROM Post_Likers GROUP BY postid) as Likes ON Likes.postid = Post.id 
   *WHERE topicid = ? 
   *ORDER BY postNumber DESC LIMIT 1;</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param topicId The topic. Must exist.
   * @return Success and a view of the latest post if one exists,
   * failure if the topic does not exist, fatal on database errors.
   */
    @Override
    public Result<PostView> getLatestPost(long topicId) {
       try{
         PreparedStatement s = c.prepareStatement(
               "SELECT forum.id as forumid,         post.topicid as topicid, " +
                      "post.id as postNumber,       person.name as authorname, " +
                      "person.username as username, post.text as text, " +
                      "post.postedAt as postedAt,       likes.numLikes as numberOfLikes " +
                      "FROM Post " +
               "JOIN Person ON Post.authorid = Person.id " +
               "JOIN Topic ON Post.topicid = Topic.id " +
               "JOIN Forum ON Topic.forumid = Forum.id " +
               "JOIN (SELECT postid, count(*) as numLikes FROM Post_Likers GROUP BY postid) as Likes ON Likes.postid = Post.id " +
               "WHERE topicid = ? " +
               "ORDER BY postNumber DESC LIMIT 1;"
            );
         if(!doesTopicExist(topicId)) return Result.failure("Topic does not exist");
         s.setLong(1, topicId);
         ResultSet r = s.executeQuery();

         //PostView(long forumId, long topicId, int postNumber, String authorName, String authorUserName, String text, int postedAt, int likes)
         PostView pv = new PostView(r.getLong("forumid"),
                                    r.getLong("topicid"),
                                    r.getInt("postNumber"),
                                    r.getString("authorname"),
                                    r.getString("username"),
                                    r.getString("text"),
                                    r.getInt("postedAt"),
                                    r.getInt("numberOfLikes"));
         return Result.success(pv);
       }catch(SQLException ex){
         printError("Error in getSimpleTopic: " + ex.getMessage());
       }
       return Result.fatal("Fatal getSimpleTopic");
    }

/*
.header on
.mode column
SELECT lastTopic.id AS topicid, lastTopic.forumid AS topicForumid, lastTopic.title AS topicTitle, f.title AS title, f.id AS id
FROM Forum AS f
JOIN (
   SELECT t.id AS id, t.forumid AS forumid, t.title AS title, p.postedAt AS postedAt
   FROM Topic t
   JOIN Post AS p ON t.id = p.topicid
   GROUP BY p.postedAt
) AS lastTopic ON f.id = lastTopic.forumid
GROUP BY f.title;

BELOW IS THE SHORTENED QUERY WITHOUT SELECTING lastTopic.forumid BECAUSE IT IS THE SAME AS f.id.

.header on
.mode column
SELECT lastTopic.id AS topicid, lastTopic.title AS topicTitle, f.title AS title, f.id AS id
FROM Forum AS f
JOIN (
   SELECT t.id AS id, t.forumid AS forumid, t.title AS title, p.postedAt AS postedAt
   FROM Topic t
   JOIN Post AS p ON t.id = p.topicid
   GROUP BY p.postedAt
) AS lastTopic ON f.id = lastTopic.forumid
GROUP BY f.title;


Test with:
http://localhost:8000/forums
*/
   /**
   * <p>Get the "main page" containing a list of forums ordered alphabetically
   * by title.</p>
   * <p><b>Visual Test:</b> http://localhost:8000/forums</p>
   * <p><b>Main Contributor:</b> Alex</p>
   * <p><b>SQL:</b> SELECT lastTopic.id AS topicid, lastTopic.title AS topicTitle, f.title AS title, f.id AS id
   * FROM Forum AS f
   * JOIN (
   * SELECT t.id AS id, t.forumid AS forumid, t.title AS title, p.postedAt AS postedAt
   * FROM Topic t
   * JOIN Post AS p ON t.id = p.topicid
   * GROUP BY p.postedAt
   * ) AS lastTopic ON f.id = lastTopic.forumid
   * GROUP BY f.title;</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @return the list of all forums, empty list if there are none.
   */    
    @Override
    public Result<List<ForumSummaryView>> getForums() {
      try(
   		PreparedStatement s = c.prepareStatement(
         "SELECT lastTopic.id AS topicid, lastTopic.forumid AS topicForumid, lastTopic.title AS topicTitle, f.title AS title, f.id AS id " +
         "FROM Forum AS f " +
         "JOIN ( " +
            "SELECT t.id AS id, t.forumid AS forumid, t.title AS title, p.postedAt AS postedAt " +
            "FROM Topic t " +
            "JOIN Post AS p ON t.id = p.topicid " +
            "GROUP BY p.postedAt " +
         ") AS lastTopic ON f.id = lastTopic.forumid " +
         "GROUP BY f.title;"
   		);
      ){
         ResultSet r = s.executeQuery();
         List<ForumSummaryView> forumsList = new ArrayList<>();

         while (r.next()) {
            SimpleTopicSummaryView lastTopic = new SimpleTopicSummaryView(r.getLong("topicid"),
                                                                          r.getLong("id"),
                                                                          r.getString("topicTitle"));

            ForumSummaryView fsv = new ForumSummaryView(r.getLong("id"),
                                                        r.getString("title"),
                                                        lastTopic);
            forumsList.add(fsv);
         }
         return Result.success(forumsList);

      }catch (SQLException ex) {
         printError("Error in getForums: " + ex.getMessage());
      }
      return Result.fatal("Fatal getForums");
    }

   /**
   * <p>Create a new forum.</p>
   * <p><b>Visual Test:</b> http://localhost:8000/newforum</p>
   * <p><b>Main Contributor:</b> Tom</p>
   * <p><b>SQL:</b> INSERT INTO Forum (title) VALUES (?);</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param title the title of the forum. Must not be null or empty and
   * no forum with this name must exist yet.
   * @return success if the forum was created, failure if the title was
   * null, empty or such a forum already existed; fatal on other errors.
   */
    // Test with: http://localhost:8000/newforum
    @Override
    public Result createForum(String title) {
       try( PreparedStatement createStatement = c.prepareStatement(
               "INSERT INTO Forum (title) VALUES (?);"
            );
         ){
         if(title == null) return Result.failure("Cannot have forum with null title");
         if(title.isEmpty()) return Result.failure("Cannot have forum with no title");
         createStatement.setString(1, title);
         createStatement.executeUpdate();
         c.commit();
         return Result.success();
      }catch (SQLException ex) {
         if(ex.getLocalizedMessage().contains("UNIQUE constraint failed: Forum.title"))
            return Result.failure(ex.getMessage());
         else return Result.fatal(ex.getMessage());
      }
    }

    //just for the tests
    private void deleteForum(String title) {
       try( PreparedStatement createStatement = c.prepareStatement(
               "DELETE FROM Forum WHERE title = ?;"
            );
         ){
         createStatement.setString(1, title);
         createStatement.executeUpdate();
         c.commit();
      }catch (SQLException ex) {
          try {
             c.rollback();
          } catch (SQLException ex1) {
             System.err.println("deleteForum rollback error");
          }
          System.err.println("deleteForum Error");
      }
    }

   /**
   * <h1> rewrite to not use getLocalizedMessage</h1>
   * <p>Create a post in an existing topic.</p>
   * <p><b>Visual Test:</b> http://localhost:8000/newpost/1</p>
   * <p><b>Main Contributor:</b> Alex</p>
   * <p><b>SQL:</b> INSERT INTO Post (authorid, topicid, text) VALUES ((SELECT id FROM Person WHERE username = ?), ?, ?);</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param topicId the id of the topic to post in. Must refer to
   * an existing topic.
   * @param username the name under which to post; user must exist.
   * @param text the content of the post, cannot be empty.
   * @return success if the post was made, failure if any of the preconditions
   * were not met and fatal if something else went wrong.
   */    
    @Override
    public Result createPost(long topicId, String username, String text) {
       try( PreparedStatement createStatement = c.prepareStatement(
          "INSERT INTO Post (authorid, topicid, text) " +
          "VALUES ((SELECT id FROM Person WHERE username = ?), ?, ?);"
          );
       ){
          if(username == null || text == null) return Result.failure("Cannot have null");
          // Error message on website says "Error - missing 'text'". Different to below?
          if(username.isEmpty() || text.isEmpty()) return Result.failure("Cannot have empty");

          createStatement.setString(1, username);
          createStatement.setLong(2, topicId);
          createStatement.setString(3, text);

          createStatement.executeUpdate();
          c.commit();

          return Result.success();
       }
       catch (SQLException ex){
          try{
             c.rollback();
          }
          catch(SQLException e){
             System.err.println("Rollback Error");
             throw new RuntimeException("Rollback Error");
          }
          if(ex.getLocalizedMessage().contains("FOREIGN KEY constraint failed"))
            return Result.failure(ex.getLocalizedMessage());
          else if(ex.getLocalizedMessage().contains("NOT NULL constraint failed: Post.authorid"))
            return Result.failure(ex.getLocalizedMessage());
          else return Result.fatal("Create post fatal error");
       }
    }

    private void deletePost(long authorid, long topicid, String text) {
       try( PreparedStatement createStatement = c.prepareStatement(
               "DELETE FROM Post WHERE authorid = ? AND topicid = ? AND text = ?;"
            );
         ){
         createStatement.setLong(1, authorid);
         createStatement.setLong(2, topicid);
         createStatement.setString(3, text);
         createStatement.executeUpdate();
         c.commit();
      }catch (SQLException | RuntimeException ex) {
          System.err.println("deletePost Error. " + ex.getLocalizedMessage());
      }
    }

   /**
   * <p>Create a new person.</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> Tom</p>
   * <p><b>SQL:</b> INSERT INTO Person (name, username, stuId) VALUES (?,?,?);</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param name the person's name, cannot be empty.
   * @param username the person's username, cannot be empty.
   * @param studentId the person's student id. May be either NULL if the
   * person is not a student or a non-empty string if they are; can not be
   * an empty string.
   * @return Success if no person with this username existed yet and a new
   * one was created, failure if a person with this username already exists,
   * fatal if something else went wrong.
   */
    @Override
    public Result addNewPerson(String name, String username, String studentId) {
       try(
               PreparedStatement s = c.prepareStatement("INSERT INTO Person (name, username, stuId) VALUES (?,?,?);");
           ){
          if(name == null || username == null) throw new RuntimeException("Cannot have null");
          if(name.isEmpty() || username.isEmpty()) throw new RuntimeException("Cannot have empty");
          if("".equals(studentId)) throw new RuntimeException("Cannot have empty studentID. Must be null or valid.");
          if(doesPersonExist(username)) return Result.failure("Username already exists");
          s.setString(1, name);
          s.setString(2, username);
          s.setString(3, studentId);
          s.executeUpdate();
          c.commit();
          return Result.success();
       }catch(RuntimeException ex){
          Result.fatal("Invalid arguments to addNewPerson");
       } catch (SQLException ex) {
          System.err.println("" + ex.getLocalizedMessage());
          Result.fatal("SQL Exception to addNewPerson");
       }
        return Result.fatal("not implemented yet");
    }
    private void deletePerson(String username){
       try( PreparedStatement createStatement = c.prepareStatement(
               "DELETE FROM Person WHERE username = ?;"
            );
         ){
         createStatement.setString(1, username);
         createStatement.executeUpdate();
         c.commit();
      }catch (SQLException | RuntimeException ex) {
         try{
            c.rollback();
         }catch(SQLException e){
            System.err.println("Rollback failed in deletePerson");
         }
          System.err.println("deletePerson Error. " + ex.getLocalizedMessage());
      }
    }

    
   /**
   * <p>Get the detailed view of a single forum.</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> Tom</p>
   * <p><b>SQL:</b> SELECT id as topicid, title as topictitle FROM Topic WHERE forumid = ?;</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param id the id of the forum to get.
   * @return A view of this forum if it exists, otherwise failure.
   */
    @Override
    public Result<ForumView> getForum(long id) {
       //public ForumView(long id, String title, List<SimpleTopicSummaryView> topics)
       //public SimpleTopicSummaryView(long topicId, long forumId, String title)
       if(!doesForumExist(id)) return Result.failure("Forum does not exist");
       
       try(
   		PreparedStatement s = c.prepareStatement(
          "SELECT id as topicid, title as topictitle FROM Topic WHERE forumid = ?;"
   	      );
       ){
         s.setLong(1, id);
         ResultSet r = s.executeQuery();

         List<SimpleTopicSummaryView> topics = new ArrayList<>();
         while (r.next()) {
            SimpleTopicSummaryView topic = new SimpleTopicSummaryView(r.getLong("topicid"),
                                                                      id,
                                                                      r.getString("topicTitle"));
            topics.add(topic);
         }

         ForumView fv = new ForumView(id, getForumTitle(id), topics);
         return Result.success(fv);
         }catch (SQLException ex) {
            printError("Error in getForums: " + ex.getMessage());
            return Result.fatal("Fatal error getForum()");
         }
    }
    
    private String getForumTitle(long id){
       try(
   		PreparedStatement s = c.prepareStatement(
          "SELECT title FROM Forum WHERE id = ?;"
   	      );
       ){
         s.setLong(1, id);
         ResultSet r = s.executeQuery();
         return r.getString("title");

         }catch (SQLException ex) {
            printError("Error in getForums: " + ex.getMessage());
            return null;
         }
    }

   /**
   * <p>Get the detailed view of a topic.</p>
   * <p><b>Visual Test:</b>http://localhost:8000/topic/4</p>
   * <p><b>Main Contributor:</b> Joseph</p>
   * <p><b>SQL:</b> </p>
   * <p>
   * <b>How it works: The base case (without page number) is fairly simple until counting post likes comes in. Since the post id is unique, this was accomplished by using a GROUP BY postid together with outer joining the post_likers table and COUNTing the number of times the postid is referred to in the post_likers table. Page numbers are handled in java by manipulating the arrayList</b>
   * 
   * 
   * </p>
   * @param topicId the topic to get.
   * @param page if 0, fetch all posts, if n > 0, fetch posts
   * 10*(n-1)+1 up to 10*n, where the first post is number 1.
   * @return The topic view if one exists with the given id and range,
   * (i.e. for getTopic(tid, 3) there must be at least 31 posts)
   * otherwise failure (or fatal on database errors). 
   */
    @Override
    public Result<TopicView> getTopic(long topicId, int page) {
       try(
         PreparedStatement s = c.prepareStatement(
            "SELECT Topic.id AS topicid, Forum.id AS forumid, Forum.title AS forumtitle, Topic.title AS topictitle, Person.name, Person.username, Post.text, Post.postedAt, COUNT(Post_Likers.postid) AS likes " +
            "FROM Topic " +
            "JOIN Forum ON Topic.forumid = Forum.id " +
            "JOIN Post ON Topic.id = Post.topicid " +
            "JOIN Person ON Post.authorid = Person.id " +
            "LEFT OUTER JOIN Post_Likers ON Post.id = Post_Likers.postid " +
            "WHERE Topic.id = ? " +
            "GROUP BY Post.id " +            
            "ORDER BY Post.postedAt ASC;"
   		);
         ){
         s.setLong(1, topicId);
         ResultSet r = s.executeQuery();
         
         ArrayList<PostView> posts  = new ArrayList<>();
         int postNumber = 0;
         long forumId;
         String forumName;
         String topicName;
         if(!r.next()){
            return Result.failure("Failure in getTopic, topic does not exist.");
         }
         else{
            postNumber++;
            PostView post = new PostView(r.getLong("forumid"), topicId, postNumber, r.getString("name"), r.getString("username"), r.getString("text"), r.getInt("postedAt"), r.getInt("likes"));
            posts.add(post);
            forumId = r.getLong("forumid");
            forumName = r.getString("forumtitle");
            topicName = r.getString("topictitle");
         }
         
         while(r.next()){
            postNumber++;
            PostView post = new PostView(r.getLong("forumid"), topicId, postNumber, r.getString("name"), r.getString("username"), r.getString("text"), r.getInt("postedAt"), r.getInt("likes"));
            posts.add(post);
         }
         
         if(page != 0){
            if(posts.size() <= 10*page){
               return Result.failure("Not enough posts in range");
            }
            else{
               ArrayList<PostView> temp = new ArrayList<>(posts.subList((10*(page-1))+1,(page*10)+1));
               posts = temp;
            }
         }
         
         TopicView topics = new TopicView(forumId, topicId, forumName, topicName, posts, page);
         return Result.success(topics);
   	}catch (SQLException ex) {
         printError("Error in getTopic: " + ex.getMessage());
   	}
      return Result.fatal("Fatal getTopic");
    }

   /**
   * <p>Like or unlike a topic. A topic is either liked or not, when calling this
   * twice in a row with the same parameters, the second call is a no-op (this
   * function is idempotent).</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> Joseph</p>
   * <p><b>SQL:</b> </p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param username the person liking the topic (must exist).
   * @param topicId the topic to like (must exist).
   * @param like true to like, false to unlike.
   * @return success (even if it was a no-op), failure if the person or topic
   * does not exist and fatal in case of db errors.
   */
    @Override
    public Result likeTopic(String username, long topicId, boolean like) {
      if(!doesTopicExist(topicId) || !doesPersonExist(username)){
         return Result.failure("Failure in likeTopic, person or topic does not exist");
      }
      
      if(like == false){
         try(
   		PreparedStatement s = c.prepareStatement(
            "DELETE FROM Topic_Likers " +
            "WHERE topicid = ? AND personid = ( " +
            "SELECT personid " +
            "FROM Topic_Likers " +
            "JOIN Person ON Topic_Likers.personid = Person.id " +          
            "WHERE Person.username = ?);"
   		);
         ){
            s.setLong(1, topicId);
            s.setString(2, username);
            s.executeUpdate();
            c.commit();
            return Result.success();
         }
         catch (SQLException ex) {
            try{c.rollback();
            }
            catch (SQLException f) {
               printError("Could not rollback, check database");
            }
            printError("Error in likeTopic: " + ex.getMessage());
         }
      }
      else{
         if(doesTopicLikeExist(username, topicId)){
            return Result.success();
         }
         try(
   		PreparedStatement s = c.prepareStatement(
            "INSERT INTO Topic_Likers (topicid, personid) " +
            "VALUES (?, (" +
            "SELECT personid " +
            "FROM Topic_Likers " +
            "JOIN Person ON Topic_Likers.personid = Person.id " +          
            "WHERE Person.username = ?));"
   		);
         ){
            s.setLong(1, topicId);
            s.setString(2, username);
            s.executeUpdate();
            c.commit();
            return Result.success();
         }
         catch (SQLException ex) {
            try{c.rollback();
            }
            catch (SQLException f) {
               printError("Could not rollback, check database");
            }
            printError("Error in likeTopic: " + ex.getMessage());
         }
      }
      return Result.fatal("Fatal likeTopic");
   }

   /**
   * <p>Set or unset a topic as favourite. Same semantics as likeTopic.</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> </p>
   * <p><b>SQL:</b> </p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param username the person setting the favourite topic (must exist).
   * @param topicId the topic to set as favourite (must exist).
   * @param fav true to set, false to unset as favourite.
   * @return success (even if it was a no-op), failure if the person or topic
   * does not exist and fatal in case of db errors.
   */    
    @Override
    public Result favouriteTopic(String username, long topicId, boolean fav) {
      if(!doesTopicExist(topicId) || !doesPersonExist(username)){
         return Result.failure("Failure in favouriteTopic, person or topic does not exist");
      }
      
      if(fav == false){
         try(
   		PreparedStatement s = c.prepareStatement(
            "DELETE FROM Topic_Fav " +
            "WHERE topicid = ? AND personid = ( " +
            "SELECT personid " +
            "FROM Topic_Fav " +
            "JOIN Person ON Topic_Fav.personid = Person.id " +          
            "WHERE Person.username = ?);"
   		);
         ){
            s.setLong(1, topicId);
            s.setString(2, username);
            s.executeUpdate();
            c.commit();
            return Result.success();
         }
         catch (SQLException ex) {
            try{c.rollback();
            }
            catch (SQLException f) {
               printError("Could not rollback, check database");
            }
            printError("Error in favouriteTopic: " + ex.getMessage());
         }
      }
      else{
         if(doesTopicFavExist(username, topicId)){
            return Result.success();
         }
         try(
   		PreparedStatement s = c.prepareStatement(
            "INSERT INTO Topic_Fav (topicid, personid) " +
            "VALUES (?, (" +
            "SELECT personid " +
            "FROM Topic_Fav " +
            "JOIN Person ON Topic_Fav.personid = Person.id " +          
            "WHERE Person.username = ?));"
   		);
         ){
            s.setLong(1, topicId);
            s.setString(2, username);
            s.executeUpdate();
            c.commit();
            return Result.success();
         }
         catch (SQLException ex) {
            try{c.rollback();
            }
            catch (SQLException f) {
               printError("Could not rollback, check database");
            }
            printError("Error in likeTopic: " + ex.getMessage());
         }
      }
      return Result.fatal("Fatal likeTopic");
   }

   /**
   * <h1>DELETETOPIC NEEDS TO ROLLBACK</h1>
   * <p>Create a new topic in a forum.</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> Tom</p>
   * <p><b>SQL1:</b> INSERT INTO Topic (forumid, title) VALUES (?,?);</p>
   * <p><b>SQL2:</b> INSERT INTO Post (authorid, topicid, text) VALUES ( 
   * (SELECT id FROM Person WHERE username = ?),
   * (SELECT id FROM Topic WHERE title = ?),
   * ?);</p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param forumId the id of the forum in which to create the topic. This
   * forum must exist.
   * @param username the username under which to make this post. Must refer
   * to an existing username.
   * @param title the title of this topic. Cannot be empty.
   * @param text the text of the initial post. Cannot be empty.
   * @return failure if any of the preconditions are not met (forum does not
   * exist, user does not exist, title or text empty);
   * success if the post was created and fatal if something else went wrong.
   */
    @Override
    public Result createTopic(long forumId, String username, String title, String text) {
       //Create Topic
       //Create first post
       //If fail to create post roll back to before create topic
       try(
               PreparedStatement newTopic = c.prepareStatement(
                  "INSERT INTO Topic (forumid, title) VALUES (?,?);"
               );
               PreparedStatement newPost = c.prepareStatement(
                  "INSERT INTO Post (authorid, topicid, text) VALUES ("
                + "(SELECT id FROM Person WHERE username = ?),"
                + "(SELECT id FROM Topic WHERE title = ?),"
                + "?);"
               )
            ){
         if(username == null || title == null || text == null) throw new RuntimeException("Cannot have null");
         if(username.isEmpty() || title.isEmpty() || text.isEmpty()) throw new RuntimeException("Cannot have empty");
         newTopic.setLong(1, forumId);
         newTopic.setString(2, title);
         newPost.setString(1, username);
         newPost.setString(2, title);
         newPost.setString(3, text);
         newTopic.executeUpdate();
         newPost.executeUpdate();
         c.commit();
         return Result.success();
       }
       catch (SQLException ex){
          try{
             c.rollback();
          }
          catch(SQLException e){
             System.err.println("Rollback Error");
             throw new RuntimeException("Rollback Error");
          }
          if(ex.getLocalizedMessage().contains("FOREIGN KEY constraint failed"))
            return Result.failure(ex.getLocalizedMessage());
          else if(ex.getLocalizedMessage().contains("NOT NULL constraint failed: Post.authorid"))
            return Result.failure(ex.getLocalizedMessage());
          else return Result.fatal("create topic failed");
       } catch(RuntimeException ex){
          try{
             c.rollback();
          }
          catch(SQLException e){
             System.err.println("Rollback Error");
             throw new RuntimeException("Rollback Error");
          }
          return Result.failure("create topic failed");
       }
    }

    private void deleteTopic(long forumId, String title){
       try( PreparedStatement createStatement = c.prepareStatement(
               "DELETE FROM Topic WHERE forumid = ? AND title = ?;"
            );
         ){
         createStatement.setLong(1, forumId);
         createStatement.setString(2, title);
         createStatement.executeUpdate();
         c.commit();
      }catch (SQLException | RuntimeException ex) {
          System.err.println("deleteForum Error. " + ex.getLocalizedMessage());
      }
    }

   /**
   * <p>Get the "main page" containing a list of forums ordered alphabetically
   * by title. Advanced version.</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> </p>
   * <p><b>SQL:</b> </p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @return the list of all forums.
   */    
    @Override
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

   /**
   * <p>Get an AdvancedPersonView for the person with the given username.</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> </p>
   * <p><b>SQL:</b> </p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param username the username to search for, cannot be empty.
   * @return If a person with the given username exists, a fully populated
   * AdvancedPersonView. Otherwise, failure (or fatal on a database error).
   */    
    @Override
    public Result<AdvancedPersonView> getAdvancedPersonView(String username) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

   /**
   * <p>Get the detailed view of a single forum, advanced version.</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> </p>
   * <p><b>SQL:</b> </p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param id the id of the forum to get.
   * @return A view of this forum if it exists, otherwise failure.
   */    
    @Override
    public Result<AdvancedForumView> getAdvancedForum(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

   /**
   * <p>Like or unlike a post. Liking a post that you have already liked
   * (or unliking a post you haven't liked) is a no-op, not an error.</p>
   * <p><b>Visual Test:</b> </p>
   * <p><b>Main Contributor:</b> Alex</p>
   * <p><b>SQL:</b> </p>
   * <p>
   * <b>How it works:</b>
   * 
   * 
   * </p>
   * @param username the person liking/unliking the post. Must exist.
   * @param topicId the topic with the post to (un)like. Must exist.
   * @param post the index of the post to (un)like. Must exist.
   * @param like true to like, false to unlike.
   * @return failure if the person or post referenced to not exist,
   * success if the (un)like succeeded, fatal in case of other errors.
   */    
    @Override
    public Result likePost(String username, long topicId, int post, boolean like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
   
    private void printError(String s){
       System.err.println(s);
    }

    private void printDebug(String s){
       System.out.println("\\x1b[32m" + s + "\\x1b[0m");
    }

    /*SELECT COUNT Topic.id
      FROM Topic
      WHERE Topic.id = 10;*/
    private boolean doesTopicExist(long topicId){
       try(
            PreparedStatement s = c.prepareStatement(
               "SELECT Topic.id " +
               "FROM Topic " +
               "WHERE Topic.id = ?"
            );
         ){
         s.setLong(1, topicId);
         ResultSet r = s.executeQuery();
         if(r.next()){
            return true;
         }
         else{
            return false;
         }
      }catch (SQLException ex) {
         printError("Error while querying if topic exists: " + ex.getMessage());
         return false;
      }
    }
    
    private boolean doesForumExist(long forumId){
       try(
            PreparedStatement s = c.prepareStatement(
               "SELECT Forum.id " +
               "FROM Forum " +
               "WHERE Forum.id = ?"
            );
         ){
         s.setLong(1, forumId);
         ResultSet r = s.executeQuery();
         if(r.next()){
            return true;
         }
         else{
            return false;
         }
      }catch (SQLException ex) {
         printError("Error while querying if topic exists: " + ex.getMessage());
         return false;
      }
    }
    
    private boolean doesPersonExist(String username){
       try(
            PreparedStatement s = c.prepareStatement(
               "SELECT Person.id " +
               "FROM Person " +
               "WHERE Person.username = ?"
            );
         ){
         s.setString(1, username);
         ResultSet r = s.executeQuery();
         if(r.next()){
            return true;
         }
         else{
            return false;
         }
      }catch (SQLException ex) {
         printError("Error while querying if person exists: " + ex.getMessage());
         return false;
      }
    }
    
    private boolean doesTopicLikeExist(String username, long topicId){
      try(
            PreparedStatement s = c.prepareStatement(
               "SELECT topicid, personid " +
               "FROM Topic_Likers " +      
               "WHERE topicid = ? AND personid = ( " +
               "SELECT personid " +
               "FROM Topic_Likers " +
               "JOIN Person ON Topic_Likers.personid = Person.id " +          
               "WHERE Person.username = ?);"
            );
         ){
         s.setLong(1, topicId);
         s.setString(2, username);
         ResultSet r = s.executeQuery();
         if(r.next()){
            return true;
         }
         else{
            return false;
         }
      }catch (SQLException ex) {
         printError("Error while querying if like exists: " + ex.getMessage());
         return false;
      }
    }
    
    private boolean doesTopicFavExist(String username, long topicId){
      try(
            PreparedStatement s = c.prepareStatement(
               "SELECT topicid, personid " +
               "FROM Topic_Fav " +      
               "WHERE topicid = ? AND personid = ( " +
               "SELECT personid " +
               "FROM Topic_Fav " +
               "JOIN Person ON Topic_Fav.personid = Person.id " +          
               "WHERE Person.username = ?);"
            );
         ){
         s.setLong(1, topicId);
         s.setString(2, username);
         ResultSet r = s.executeQuery();
         if(r.next()){
            return true;
         }
         else{
            return false;
         }
      }catch (SQLException ex) {
         printError("Error while querying if favourite exists: " + ex.getMessage());
         return false;
      }
    }
}
