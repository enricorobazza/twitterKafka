import pandas as pd
import io
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from joblib import dump, load

class SentimentPrediction:
    def __init__(self):
        self.df = pd.read_csv("imdb-reviews-pt-br.csv").sample(1000, random_state=42)
        self.df.sentiment = self.df['sentiment'].map({'pos': 1, 'neg': 0})
        try:
            self.clf = load('model.joblib') 
        except OSError as e:
            print("Model not found, creating...")
            

            vect = CountVectorizer(ngram_range=(1, 1))
            vect.fit(self.df.text_pt)
            text_vect = vect.transform(self.df.text_pt)

            X_train,X_test,y_train,y_test = train_test_split(
                text_vect, 
                self.df.sentiment,
                test_size = 0.3, 
                random_state = 42
            )

            self.clf = LogisticRegression(random_state=0, solver='newton-cg')
            self.clf = self.clf.fit(X_train, y_train)
            dump(self.clf, 'model.joblib') 


    def predict_text(self, texto):
        vect = CountVectorizer(ngram_range=(1, 1))
        vect.fit(self.df.text_pt)
        text_vect = vect.transform([texto])
        y_pred = self.clf.predict(text_vect)
        return y_pred
        # if y_pred[0] == 1:
        #     return "Positivo"
        # else:
        #     return "Negativo"