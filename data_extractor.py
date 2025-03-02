import pandas as pd
import regex
import emoji

class DataExtractor:
    def __init__(self, source_file: str, chunksize: int = 100000):
        """
        Inicializa el extractor con el archivo de origen.
        Parámetro:
            source_file: Ruta al archivo de datos (CSV o JSON).
            chunksize: Tamaño de los fragmentos de datos a cargar.
        """
        self.source_file = source_file
        self.data = None
        self.chunksize = chunksize

    def load_data(self):
        """
        Carga los datos y se queda con el campo del texto como "text" y el campo del usuario como "user_name".
        Elimina el resto de los campos.
        """
        try:
            self.data = pd.read_csv(
                self.source_file,
                usecols=["text", "user_name"],
                chunksize=self.chunksize,
                encoding='utf-8',
                sep=',',
                on_bad_lines='skip',
                engine='python'
            )
        except Exception as e:
            print(f"Error al cargar el archivo: {e}")
            self.data = []
        return self.data

    def clean_text(self, text: str) -> str:
        if not isinstance(text, str):
            return ""
        text = regex.sub(r'[^\w\s#@:$%.?!\U0001F600-\U0001F64F]+', '', text)
        text = regex.sub(r'\s+', ' ', text).strip()
        return text

    def extract_hashtags(self, text: str) -> list:
        return regex.findall(r'#\w+', text, regex.UNICODE)

    def extract_urls(self, text: str) -> list:
        return regex.findall(r'https?://[^\s]+|https?:[^\s]+', text)

    def extract_price(self, text: str) -> list:
        return [float(price.replace(',', '').strip()[1:]) for price in regex.findall(r'[\$\€\£]\s?\d+(?:,\d{3})*(?:\.\d+)?', text)]

    def extract_emoticons(self, text: str) -> list:
        return [match["emoji"] for match in emoji.emoji_list(text)]

    def extract_mentions(self, text: str) -> list:
        return regex.findall(r'@(\w+)', text)

    def process_text(self) -> pd.DataFrame:
        df_list = []
        for chunk in self.load_data():
            chunk["text"] = chunk["text"].astype(str).apply(self.clean_text)
            chunk["Hashtags"] = chunk["text"].apply(self.extract_hashtags)
            chunk["URLs"] = chunk["text"].apply(self.extract_urls)
            chunk["Prices"] = chunk["text"].apply(self.extract_price)
            chunk["Emoticons"] = chunk["text"].apply(self.extract_emoticons)
            chunk["Mentions"] = chunk["text"].apply(self.extract_mentions)
            df_list.append(chunk)

        if df_list:
            self.df_data = pd.concat(df_list, ignore_index=True)
        else:
            self.df_data = pd.DataFrame(columns=["text", "user_name", "Hashtags", "URLs", "Prices", "Emoticons", "Mentions"])      
        return self.df_data

    def save_file(self, file_name: str) -> None:
        if self.df_data is not None and not self.df_data.empty:
            self.df_data.to_csv(file_name + ".csv", index=False)
        else:
            print("No hay datos para guardar.")
