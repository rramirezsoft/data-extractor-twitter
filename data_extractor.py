import pandas as pd
import regex


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
        """
        Limpiamos el texto del dataset eliminando carcateres especiales y espacios redundantes.
        """
        if text is not None:     
            text = regex.sub(
                r'[^\w\s#@:$%.?!\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F700-\U0001F77F]+', '',
                text)
            text = regex.sub(r'\s+', ' ', text).strip()
            return text
        return ""

    def extract_hashtags(self, text) -> list:
        """
        Extrae hashtags del texto asegurando que no se capturen caracteres extraños.
        """
        if not isinstance(text, str) or pd.isna(text):
            return []

        return regex.findall(r'#\b[a-zA-Z0-9_]+\b', text)

    def extract_urls(self, text: str) -> list:
        """
        Extrae URLs del texto, capturando sólo las reales.
        """
        if not isinstance(text, str) or pd.isna(text): 
            return []
        return regex.findall(r'https?://\S+', text) 

    def extract_price(self, text: str) -> list:
        """
        Extrae precios del texto con $ al principio o al final.
        """
        if not isinstance(text, str) or pd.isna(text):  # Si no es string o es NaN, devolver lista vacía
            return []

        prices = regex.findall(r"(?:\$\s?([\d]+(?:,\d{3})*(?:\.\d+)?)|\b([\d]+(?:,\d{3})*(?:\.\d+)?)\$)", text)

        prices = [p[0] if p[0] else p[1] for p in prices]

        return [float(price.replace(",", "")) for price in prices]

    def extract_emoticons(self, text: str) -> list:
        """
        Extrae emoticonos del texto.
        """
        if not isinstance(text, str) or pd.isna(text):
            return []

        # Detectamos los emojis de presentación
        emoji_pattern = regex.compile(r"(\p{Emoji_Presentation}+)")

        # Agrupamos los emojis consecutivos
        emoticons = ["".join(match) for match in emoji_pattern.findall(text)]

        # Eliminamos las tonalidades para obtener los emoticonos únicos
        skin_tones = ["🏻", "🏼", "🏽", "🏾", "🏿"]
        emoticons = ["".join(c for c in emoji if c not in skin_tones) for emoji in emoticons]

        return emoticons if emoticons else []

    def extract_mentions(self, text: str) -> list:
        """
        Extrae menciones del texto.
        """
        if not isinstance(text, str) or pd.isna(text):
            return []
        return regex.findall(r'@\w+', text)

    def process_text(self) -> pd.DataFrame:
        """
        Extrae todos los componentes del texto y devuelve el DataFrame procesado.
        """
        df_list = []
        for chunk in self.load_data():  # Si los datos son de tipo chunk
            chunk["Hashtags"] = chunk["text"].apply(self.extract_hashtags)
            chunk["URLs"] = chunk["text"].apply(self.extract_urls)
            chunk["Prices"] = chunk["text"].apply(self.extract_price)
            chunk["Emoticons"] = chunk["text"].apply(self.extract_emoticons)
            chunk["Mentions"] = chunk["text"].apply(self.extract_mentions)
            chunk["text_cleaned"] = chunk["text"].apply(self.clean_text)
            df_list.append(chunk)
        self.df_data = pd.concat(df_list, ignore_index=True)
        return self.df_data

    def save_file(self, file_name: str) -> None:
        if self.df_data is None:
            self.process_text()
        self.df_data.to_csv(file_name + ".csv", index=False)
