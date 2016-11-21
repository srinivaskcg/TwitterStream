import java.util.HashMap;
import java.util.Map;


public class StateInfo {
	private String name;
	private String abbrv;
	Map<String, Integer> hashtags;
	
	StateInfo(String name, String abbrv){
		hashtags = new HashMap<String, Integer>();
		this.name = name;
		this.abbrv = abbrv;
	}
	
	String getAbrv(){
		return this.abbrv;
	}
	
	Map<String, Integer> getStateHash(){
		return this.hashtags;
	}
	
	void addHashtag(String hashtag, int count){
		if(this.hashtags.containsKey(hashtag)){
			this.hashtags.put(hashtag, this.hashtags.get(hashtag) + count);
		}else{
			this.hashtags.put(hashtag, count);
		}
	}
}
