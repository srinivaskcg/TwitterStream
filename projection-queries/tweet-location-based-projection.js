fromStream("TweetStream11").when({
    $init : function(s, e){
        return {arr : [], count : 0};
    },
    $any : function(s,e){
        var getState = function(city_state){
            city = city_state.slice(0,city_state.lastIndexOf(","))
            state = city_state.slice(city_state.lastIndexOf(",")+1);
            return {"city":city.trim(), "state":state.trim()};
        }
        
        if(e !== null && typeof(e.body) != "undefined" && e.body !== null && e.body.hasOwnProperty("place") && e.body.place !== null){
            if(e.body.place.full_name!== null){
                s.arr.push(e.body.place.full_name);
                hashtagsArr = e.body.entities.hashtags;
                hashtags = {}
                flag = false;
                for(var item in hashtagsArr){
                    hashtags[hashtagsArr[item].text] = 1
                    flag = true;
                }
                
                city_state = getState(e.body.place.full_name)
                // linkTo(city_state.state, e);
                if(flag){
                    pojo = {};
                    pojo["city"] = city;
                    pojo["hashtags"] = hashtags;
                    emit(city_state.state,"events_by_state", pojo);
                }
                s.count++;
            }
        }else{
            // emit("karthik1", e.body);
        }
        return s;
    }   
}); 